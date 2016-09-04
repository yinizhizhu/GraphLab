#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>

typedef int color_type;

struct vertex_data: graphlab::IS_POD_TYPE {
	color_type color;
	vertex_data(color_type color = std::numeric_limits<color_type>::max()) :
		color(color) {
	}
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct min_color_type: graphlab::IS_POD_TYPE {
	color_type color;
	min_color_type(color_type color =
			std::numeric_limits<color_type>::max()) :
				color(color) {
	}
	min_color_type& operator+=(const min_color_type& other) {
		color = std::min(color, other.color);
		return *this;
	}
};

// gather type is graphlab::empty, then we use message model
class cc: public graphlab::ivertex_program<graph_type, min_color_type>,
		public graphlab::IS_POD_TYPE {
	bool changed;
public:

	edge_dir_type gather_edges(icontext_type& context,
			const vertex_type& vertex) const {
		return graphlab::ALL_EDGES;
	}
	min_color_type gather(icontext_type& context, const vertex_type& vertex,
			edge_type& edge) const {
		return min_color_type(edge.source().data().color);
	}

	void apply(icontext_type& context, vertex_type& vertex,
			const gather_type& total) {
		changed = false;
		if (vertex.data().color > total.color) {
			changed = true;
			vertex.data().color = total.color;
		}
	}

	edge_dir_type scatter_edges(icontext_type& context,
			const vertex_type& vertex) const {
		if (changed)
			return graphlab::ALL_EDGES;
		else
			return graphlab::NO_EDGES;
	}
	; // end of scatter_edges

	/**
	 * \brief The scatter function just signal adjacent pages
	 */
	void scatter(icontext_type& context, const vertex_type& vertex,
			edge_type& edge) const {
		const vertex_type other = edge.target();
		context.signal(other);
	}

};

struct cc_writer {
	std::string save_vertex(const graph_type::vertex_type& vtx) {
		std::stringstream strm;
		strm << vtx.id() << "\t" << vtx.data().color << "\n";
		if (vtx.data().color == std::numeric_limits<color_type>::max())
			return "";
		else
			return strm.str();
	}
	std::string save_edge(graph_type::edge_type e) {
		return "";
	}
};

bool line_parser(graph_type& graph, const std::string& filename,
		const std::string& textline) {

	std::istringstream ssin(textline);
	graphlab::vertex_id_type vid;
	ssin >> vid;
    int out_nb;
	ssin >> out_nb;
	if(out_nb == 0)
       graph.add_vertex(vid);
    while (out_nb--) {
		graphlab::vertex_id_type other_vid;
		ssin >> other_vid;
		graph.add_edge(vid, other_vid);
	}
	return true;
}

void init_vertex(graph_type::vertex_type& vertex) { vertex.data().color = vertex.id(); }

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);

    char *input_file = "hdfs://master:9000/pullgel/friend";
    char *output_file = "hdfs://master:9000/exp/friend";
	std::string exec_type = "asynchronous";

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
	//std::string exec_type = "synchronous";
	graphlab::omni_engine<cc> engine(dc, graph, exec_type);

	engine.signal_all();
	engine.start();

	dc.cout() << "Finished Running engine in " << engine.elapsed_seconds()
			<< " seconds." << std::endl;

	t.start();

	graph.save(output_file, cc_writer(), false, // set to true if each output file is to be gzipped
			true, // whether vertices are saved
			false); // whether edges are saved
	dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
			<< std::endl;

	graphlab::mpi_tools::finalize();
}
