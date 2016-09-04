#include <vector>
#include <string>
#include <fstream>
#include <boost/unordered_set.hpp>
#include <graphlab.hpp>

struct vertex_data: graphlab::IS_POD_TYPE
{
    int left;
    int matchTo;
    int lastMatchTo;
    vertex_data()
    {
        matchTo = -1;
        lastMatchTo = -1;
    }
    vertex_data(int left, int matchTo = -1) :
            left(left), matchTo(matchTo), lastMatchTo(-1)
    {}
}
;

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct set_union_gather
{
    boost::unordered_set<int> msgs;
    set_union_gather()
    {}
    set_union_gather(int value)
    {
        msgs.insert(value);
    }
    set_union_gather& operator+=(const set_union_gather& other)
    {
        for(boost::unordered_set<int>::iterator it = other.msgs.begin() ; it != other.msgs.end() ; it++)
        {
            msgs.insert(*it);
        }
        return *this;
    }

    // serialize
    void save(graphlab::oarchive& oarc) const
    {
        oarc << msgs;
    }

    // deserialize
    void load(graphlab::iarchive& iarc)
    {
        iarc >> msgs;
    }
};

int minValue(const boost::unordered_set<int> & msgs)
{
    int min = std::numeric_limits<int>::max();
    for (boost::unordered_set<int>::const_iterator it = msgs.begin();
            it != msgs.end(); it++)
    {
        min = std::min(min, *it);
    }
    return min;
}

int minValue(const std::vector<int> & msgs)
{
    int min = std::numeric_limits<int>::max();
    for (std::vector<int>::const_iterator it = msgs.begin();
            it != msgs.end(); it++)
    {
        min = std::min(min, *it);
    }
    return min;
}

// gather type is graphlab::empty, then we use message model
class bmm: public graphlab::ivertex_program<graph_type, graphlab::empty,
            set_union_gather>
{
    boost::unordered_set<int> msgs;
    int update;
    int minMsg;
public:

    void init(icontext_type& context, const vertex_type& vertex,
              const set_union_gather& msg)
    {
        this->msgs = msg.msgs;
        this->minMsg = minValue(msg.msgs);
        this->update = 0;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const graphlab::empty& empty)
    {
        if (context.iteration() % 4 == 0)
        {
            if(vertex.data().left == 1 && vertex.data().matchTo == -1)
            {
                update = 1;
            }
        }
        else if (context.iteration() % 4 == 1)
        {
            if(vertex.data().left == 0 && vertex.data().matchTo == -1)
            {
                if (msgs.size() > 0)
                {
                    update = 1;
                }
            }
        }
        else if (context.iteration() % 4 == 2)
        {
            if(vertex.data().left == 1 && vertex.data().matchTo == -1)
            {
                std::vector<int> grants;
                for (boost::unordered_set<int>::const_iterator it = msgs.begin();
                        it != msgs.end(); it++)
                {
                    if (*it >= 0)
                        grants.push_back(*it);
                }
                if (grants.size() > 0)
                {
                    vertex.data().matchTo = minValue(grants);
                    update = 1;
                }
            }
        }
        else if (context.iteration() % 4 == 3)
        {
            if(vertex.data().left == 0 && vertex.data().matchTo == -1)
            {
                if (msgs.size() == 1)
                {
                    vertex.data().matchTo = *msgs.begin();
                }
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (context.iteration() % 4 == 3)
        {
            return graphlab::NO_EDGES;
        }
        else
        {
            if (update)
                return graphlab::OUT_EDGES;
            else
                return graphlab::NO_EDGES;
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {

        if (context.iteration() % 4 == 0)
        {
            set_union_gather m(vertex.id());
            context.signal(edge.target(), m);
        }
        else if (context.iteration() % 4 == 1)
        {
            if (edge.target().id() == minMsg)
            {
                set_union_gather m(vertex.id());
                context.signal(edge.target(), m);
            }
            else
            {
                if (msgs.count(edge.target().id()))
                {
                    set_union_gather m(-vertex.id() - 1);
                    context.signal(edge.target(), m);
                }
            }

        }
        else if (context.iteration() % 4 == 2)
        {
            if (edge.target().id() == vertex.data().matchTo)
            {
                set_union_gather m(vertex.id());
                context.signal(edge.target(), m);
            }

        }
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << msgs;
        oarc << update;
        oarc << minMsg;
    }
    void load(graphlab::iarchive& iarc)
    {
        iarc >> msgs;
        iarc >> update;
        iarc >> minMsg;
    }

};
struct bmm_writer
{
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t" << vtx.data().matchTo << "\n";
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
    graphlab::vertex_id_type vid,other_vid;
    int left;
    ssin >> vid >> left;
    graph.add_vertex(vid, vertex_data(left == 0 ? 1 : 0, -1));
    while (ssin >> other_vid)
    {
        graph.add_edge(vid, other_vid);
    }
    return true;
}
void copy_lastmatchto(graph_type::vertex_type& vdata)
{
    vdata.data().lastMatchTo = vdata.data().matchTo;
}
int vertex_data_change(const graph_type::vertex_type& vertex)
{
    return (vertex.data().lastMatchTo == vertex.data().matchTo ? 0 : 1);
}
int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char *input_file = "hdfs://master:9000/pullgel/blivej";
    char *output_file = "hdfs://master:9000/exp/bmm";
    std::string exec_type = "synchronous";

    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    graphlab::timer t;
    t.start();
    graph_type graph(dc);
    graph.load(input_file, line_parser);
    graph.finalize();

    dc.cout() << "Loading graph in " << t.current_time() << " seconds"
    << std::endl;

    graphlab::omni_engine<bmm> engine(dc, graph, exec_type);

    t.start();
    int round = 0;
    while(true)
    {
        engine.signal_all();
        engine.start();

        int change = graph.map_reduce_vertices<int>(vertex_data_change);
        dc.cout() << change << " vertices have been matched in round " << ++ round << std::endl;
        if(change == 0)
            break;
        graph.transform_vertices(copy_lastmatchto);
    }

    dc.cout() << "Finished Running engine in " << t.current_time()
    << " seconds after " <<  round << " rounds." << std::endl;

    t.start();

    graph.save(output_file, bmm_writer(), false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
    << std::endl;

    graphlab::mpi_tools::finalize();
}
