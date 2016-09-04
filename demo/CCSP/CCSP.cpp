#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>

typedef int color_type;
const int BFS_SOURCE = 15588959; // hard code for frined
//const int BFS_SOURCE = 75525479; // hard code for btc
struct vertex_data : graphlab::IS_POD_TYPE {
    color_type color;
    vertex_data(color_type color = std::numeric_limits<color_type>::max())
        : color(color)
    {
    }
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct min_color_type : graphlab::IS_POD_TYPE {
    color_type color;
    min_color_type(color_type color = std::numeric_limits<color_type>::max())
        : color(color)
    {
    }
    min_color_type& operator+=(const min_color_type& other)
    {
        color = std::min(color, other.color);
        return *this;
    }
};

// gather type is graphlab::empty, then we use message model
class bfs : public graphlab::ivertex_program<graph_type, graphlab::empty,
                                             min_color_type>,
            public graphlab::IS_POD_TYPE {
    bool changed;
    color_type min_color;

public:
    void init(icontext_type& context, const vertex_type& vertex,
              const min_color_type& msg)
    {
        min_color = msg.color;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const graphlab::empty& empty)
    {
        if (vertex.data().color == -1)
            return;

        vertex.data().color = -1;
        changed = true;
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (changed)
            return graphlab::ALL_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {

        const vertex_type other = edge.target();
        color_type newc = vertex.data().color;
        const min_color_type msg(newc);
        context.signal(other, msg);
    }
};

// gather type is graphlab::empty, then we use message model
class cc : public graphlab::ivertex_program<graph_type, graphlab::empty,
                                            min_color_type>,
           public graphlab::IS_POD_TYPE {
    bool changed;
    color_type min_color;

public:
    void init(icontext_type& context, const vertex_type& vertex,
              const min_color_type& msg)
    {
        min_color = msg.color;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const graphlab::empty& empty)
    {
        if (vertex.data().color == -1)
            return;

        if (context.iteration() == 0) {
            vertex.data().color = vertex.id();
            changed = true;
            return;
        }

        changed = false;
        if (vertex.data().color > min_color) {
            changed = true;
            vertex.data().color = min_color;
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (vertex.data().color == -1)
            return graphlab::NO_EDGES;
        if (changed)
            return graphlab::ALL_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {

        const vertex_type other = edge.target();
        color_type newc = vertex.data().color;
        const min_color_type msg(newc);
        context.signal(other, msg);
    }
};

struct cc_writer {
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t" << vtx.data().color << "\n";
        if (vtx.data().color == std::numeric_limits<color_type>::max())
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
    if (out_nb == 0)
        graph.add_vertex(vid);
    while (out_nb--) {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        graph.add_edge(vid, other_vid);
    }
    return true;
}

int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char* input_file = argv[1];
    char* output_file = argv[2];
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

    graphlab::omni_engine<bfs> BFSEngine(dc, graph, exec_type);

    BFSEngine.signal(BFS_SOURCE);

    BFSEngine.start();

    dc.cout() << "Finished Running engine in " << BFSEngine.elapsed_seconds()
              << " seconds." << std::endl;

    graphlab::omni_engine<cc> CCEngine(dc, graph, exec_type);

    CCEngine.signal_all();

    CCEngine.start();

    dc.cout() << "Finished Running engine in " << CCEngine.elapsed_seconds()
              << " seconds." << std::endl;

    t.start();

    graph.save(output_file, cc_writer(), false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
              << std::endl;

    graphlab::mpi_tools::finalize();
}
