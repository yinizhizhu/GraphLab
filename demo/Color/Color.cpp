/*
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <boost/unordered_set.hpp>
#include <graphlab.hpp>
#include <graphlab/ui/metrics_server.hpp>
#include <graphlab/macros_def.hpp>


typedef graphlab::vertex_id_type color_type;

/*
 * no edge data
 */
typedef graphlab::empty edge_data_type;
bool EDGE_CONSISTENT = false;


/*
 * This is the gathering type which accumulates an (unordered) set of
 * all neighboring colors
 * It is a simple wrapper around a boost::unordered_set with
 * an operator+= which simply performs a set union.
 *
 * This struct can be significantly accelerated for small sets.
 * Small collections of vertex IDs should not require the overhead
 * of the unordered_set.
 */
struct set_union_gather
{
    boost::unordered_set<color_type> colors;

    /*
     * Combining with another collection of vertices.
     * Union it into the current set.
     */
    set_union_gather& operator+=(const set_union_gather& other)
    {
        foreach(graphlab::vertex_id_type othervid, other.colors)
        {
            colors.insert(othervid);
        }
        return *this;
    }

    // serialize
    void save(graphlab::oarchive& oarc) const
    {
        oarc << colors;
    }

    // deserialize
    void load(graphlab::iarchive& iarc)
    {
        iarc >> colors;
    }
};

/*
 * Define the type of the graph
 */
typedef graphlab::distributed_graph<color_type,
        edge_data_type> graph_type;


/*
 * On gather, we accumulate a set of all adjacent colors.
 */
class graph_coloring:
    public graphlab::ivertex_program<graph_type,
    set_union_gather>,
/* I have no data. Just force it to POD */
public graphlab::IS_POD_TYPE
{
public:
    // Gather on all edges
    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::ALL_EDGES;
    }

    /*
     * For each edge, figure out the ID of the "other" vertex
     * and accumulate a set of the neighborhood vertex IDs.
     */
    gather_type gather(icontext_type& context,
                       const vertex_type& vertex,
                       edge_type& edge) const
    {
        set_union_gather gather;
        color_type other_color = edge.source().id() == vertex.id() ?
                                 edge.target().data(): edge.source().data();
        // vertex_id_type otherid= edge.source().id() == vertex.id() ?
        //                              edge.target().id(): edge.source().id();
        gather.colors.insert(other_color);
        return gather;
    }

    /*
     * the gather result now contains the colors in the neighborhood.
     * pick a different color and store it
     */
    void apply(icontext_type& context, vertex_type& vertex,
               const gather_type& neighborhood)
    {
        // find the smallest color not described in the neighborhood
        size_t neighborhoodsize = neighborhood.colors.size();
        for (color_type curcolor = 0; curcolor < neighborhoodsize + 1; ++curcolor)
        {
            if (neighborhood.colors.count(curcolor) == 0)
            {
                vertex.data() = curcolor;
                break;
            }
        }
    }


    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (EDGE_CONSISTENT) return graphlab::NO_EDGES;
        else return graphlab::ALL_EDGES;
    }


    /*
     * For each edge, count the intersection of the neighborhood of the
     * adjacent vertices. This is the number of triangles this edge is involved
     * in.
     */
    void scatter(icontext_type& context,
                 const vertex_type& vertex,
                 edge_type& edge) const
    {
        // both points have different colors!
        if (edge.source().data() == edge.target().data())
        {
            context.signal(edge.source().id() == vertex.id() ?
                           edge.target() : edge.source());
        }
    }
};




/*
 * A saver which saves a file where each line is a vid / color pair
 */
struct save_colors
{
    std::string save_vertex(graph_type::vertex_type v)
    {
        return graphlab::tostr(v.id()) + "\t" +
               graphlab::tostr(v.data()) + "\n";
    }
    std::string save_edge(graph_type::edge_type e)
    {
        return "";
    }
};


/**************************************************************************/
/*                                                                        */
/*                         Validation   Functions                         */
/*                                                                        */
/**************************************************************************/
size_t validate_conflict(graph_type::edge_type& edge)
{
    return edge.source().data() == edge.target().data();
}


int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;

    dc.cout() << "This program computes a simple graph coloring of a"
              "provided graph.\n\n";

    std::string graph_dir = argv[1];
    std::string output_file = "hdfs://master:9000/exp/color_out";
    std::string format = "adj";
    std::string exec_type = "asynchronous";

    //load graph
    graphlab::timer t;
    t.start();
    graph_type graph(dc);

    dc.cout() << "Loading graph in format: " << format << std::endl;
    graph.load_format(graph_dir, format);
    graph.finalize();

    dc.cout() << "Loading graph in " << t.current_time() << " seconds"
			<< std::endl;
	graphlab::omni_engine<graph_coloring> engine(dc, graph, exec_type);

    dc.cout() << "Coloring..." << std::endl;


    engine.signal_all();
    engine.start();

    dc.cout() << "Finished Running engine in " << engine.elapsed_seconds()
			<< " seconds." << std::endl;

	t.start();

	graph.save(output_file, save_colors(), false, // set to true if each output file is to be gzipped
			true, // whether vertices are saved
			false); // whether edges are saved
	dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
			<< std::endl;

	graphlab::mpi_tools::finalize();
	return 0;
} // End of main

