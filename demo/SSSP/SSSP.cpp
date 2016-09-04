#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>

typedef double distance_type;
const int SOURCE = 0;

struct vertex_data: graphlab::IS_POD_TYPE
{
    distance_type dist;
    vertex_data(distance_type dist = std::numeric_limits<distance_type>::max()) :
        dist(dist)
    {
    }
};

struct edge_data: graphlab::IS_POD_TYPE
{
    distance_type dist;
    edge_data(distance_type dist = 1) :
        dist(dist)
    {
    }
};
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct min_distance_type: graphlab::IS_POD_TYPE
{
    distance_type dist;
    min_distance_type(distance_type dist =
                          std::numeric_limits<distance_type>::max()) :
        dist(dist)
    {
    }
    min_distance_type& operator+=(const min_distance_type& other)
    {
        dist = std::min(dist, other.dist);
        return *this;
    }
};

// gather type is graphlab::empty, then we use message model
class sssp: public graphlab::ivertex_program<graph_type, graphlab::empty,
    min_distance_type>, public graphlab::IS_POD_TYPE
{
    distance_type min_dist;
    bool changed;
public:

    void init(icontext_type& context, const vertex_type& vertex,
              const min_distance_type& msg)
    {
        min_dist = msg.dist;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const graphlab::empty& empty)
    {
        changed = false;
        if (vertex.data().dist > min_dist)
        {
            changed = true;
            vertex.data().dist = min_dist;
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (changed)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {
        const vertex_type other = edge.target();
        distance_type newd = vertex.data().dist + edge.data().dist;

        const min_distance_type msg(newd);
        context.signal(other, msg);

    }

};

struct sssp_writer
{
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t" << vtx.data().dist << "\n";
        if (vtx.data().dist == std::numeric_limits<distance_type>::max())
            return "";
        else
            return strm.str();
    }
    std::string save_edge(graph_type::edge_type e)
    {
        return "";
    }
};

bool line_parser(graph_type& graph, const std::string& filename,
                 const std::string& textline)
{

    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int out_nb;
    ssin >> out_nb;
    if(out_nb == 0)
        graph.add_vertex(vid);
    while (out_nb--)
    {
        graphlab::vertex_id_type other_vid;
        edge_data edge;
        ssin >> other_vid >> edge.dist;
        graph.add_edge(vid, other_vid, edge);
    }
    return true;
}

void init_vertex(graph_type::vertex_type& vertex)
{

    vertex.data().dist = std::numeric_limits<distance_type>::max();
}

int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);
    char *input_file = "hdfs://master:9000/pullgel/usa";
    char *output_file = "hdfs://master:9000/exp/sssp";
    std::string exec_type = "synchronous";
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    graphlab::timer t;
    t.start();
    graph_type graph(dc);
    graph.load(input_file, line_parser);
    graph.finalize();
    graph.transform_vertices(init_vertex);
    dc.cout() << "Loading graph in " << t.current_time() << " seconds"
              << std::endl;
    
    graphlab::omni_engine<sssp> engine(dc, graph, exec_type);

    engine.signal(SOURCE, min_distance_type(0));
    engine.start();

    dc.cout() << "Finished Running engine in " << engine.elapsed_seconds()
              << " seconds." << std::endl;

    t.start();

    graph.save(output_file, sssp_writer(), false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
              << std::endl;

    graphlab::mpi_tools::finalize();

}
