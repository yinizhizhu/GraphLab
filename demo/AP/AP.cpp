#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <map>
#include <time.h>

#include <graphlab.hpp>

//helper function
float myrand() {
	return static_cast<float>(rand() / (RAND_MAX + 1.0));
}

//helper function to return a hash value for Flajolet & Martin bitmask
size_t hash_value() {
	size_t ret = 0;
	while (myrand() < 0.5) {
		ret++;
	}
	return ret;
}

const size_t DUPULICATION_OF_BITMASKS = 10;
//helper function to compute bitwise-or
void bitwise_or(std::vector<int>& v1, const std::vector<int>& v2) {
	for (size_t a = 0; a < v1.size(); ++a) {
		v1[a] |= v2[a];
	}
}

struct vdata {
	//use two bitmasks for consistency
	std::vector<int> bitmask;

	//indicate which is the bitmask for reading (or writing)

	vdata() :
			bitmask() {
	}
	explicit vdata(const std::vector<int> & others) :
			bitmask() {
		bitmask.clear();
		for (size_t i = 0; i < others.size(); ++i) {
			bitmask.push_back(others[i]);
		}
	}
	vdata& operator+=(const vdata& other) {
		bitwise_or(bitmask, other.bitmask);
		return *this;
	}
	//for approximate Flajolet & Martin counting
	void create_hashed_bitmask(size_t id) {
		for (size_t i = 0; i < DUPULICATION_OF_BITMASKS; ++i) {
			size_t hash_val = hash_value();
			bitmask.push_back(1 << hash_val);
		}
	}

	void save(graphlab::oarchive& oarc) const {
		size_t num = bitmask.size();
		oarc << num;
		for (size_t a = 0; a < num; ++a) {
			oarc << bitmask[a];
		}
	}
	void load(graphlab::iarchive& iarc) {
		bitmask.clear();
		size_t num = 0;
		iarc >> num;
		for (size_t a = 0; a < num; ++a) {
			int elem;
			iarc >> elem;
			bitmask.push_back(elem);

		}
	}
};

typedef graphlab::distributed_graph<vdata, graphlab::empty> graph_type;

//initialize bitmask
void initialize_vertex_with_hash(graph_type::vertex_type& v) {
	v.data().create_hashed_bitmask(v.id());
}

//The next bitmask b(h + 1; i) of i at the hop h + 1 is given as:
//b(h + 1; i) = b(h; i) BITWISE-OR {b(h; k) | source = i & target = k}.
class one_hop: public graphlab::ivertex_program<graph_type, graphlab::empty,
		vdata> {
public:
	std::vector<int> bitmask;
	void save(graphlab::oarchive& oarc) const {
		size_t num = bitmask.size();
		oarc << num;
		for (size_t a = 0; a < num; ++a) {
			oarc << bitmask[a];
		}
	}
	void load(graphlab::iarchive& iarc) {
		bitmask.clear();
		size_t num = 0;
		iarc >> num;
		for (size_t a = 0; a < num; ++a) {
			int elem;
			iarc >> elem;
			bitmask.push_back(elem);

		}
	}
	void init(icontext_type& context, const vertex_type& vertex,
			const vdata& msg) {
		bitmask = msg.bitmask;
	}
	//gather on out edges
	edge_dir_type gather_edges(icontext_type& context,
			const vertex_type& vertex) const {
		return graphlab::NO_EDGES;
	}

	//get bitwise-ORed bitmask and switch bitmasks
	void apply(icontext_type& context, vertex_type& vertex,
			const graphlab::empty& empty) {
		if(context.iteration() == 1)
		{
			bitwise_or(vertex.data().bitmask, bitmask);
		}
	}

	edge_dir_type scatter_edges(icontext_type& context,
			const vertex_type& vertex) const {
		if(context.iteration() == 0)
		{
			return graphlab::OUT_EDGES;
		}
		else
		{
			return graphlab::NO_EDGES;
		}

	}
	void scatter(icontext_type& context, const vertex_type& vertex,
			edge_type& edge) const {
		const vertex_type other = edge.target();
		const vdata msg(vertex.data().bitmask);
		context.signal(other, msg);
	}
};

//count the number of vertices reached in the current hop with Flajolet & Martin counting method
size_t approximate_pair_number(std::vector<int> bitmask) {
	float sum = 0.0;
	for (size_t a = 0; a < bitmask.size(); ++a) {
		for (size_t i = 0; i < 32; ++i) {
			if ((bitmask[a] & (1 << i)) == 0) {
				sum += (float) i;
				break;
			}
		}
	}
	return (size_t) (pow(2.0, sum / (float) (bitmask.size())) / 0.77351);
}
//count the number of notes reached in the current hop
size_t absolute_vertex_data_with_hash(const graph_type::vertex_type& vertex) {
	size_t count = approximate_pair_number(vertex.data().bitmask);
	return count;
}

int main(int argc, char** argv) {

	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;

	float termination_criteria = 0.0001;

	std::string graph_dir = argv[1];
	std::string format = "adj";
	bool use_sketch = true;
	int round = atoi(argv[2]);
    std::string exec_type = "synchronous";

	//load graph
	graphlab::timer t;
	t.start();
	graph_type graph(dc);
	dc.cout() << "Loading graph in format: " << format << std::endl;
	graph.load_format(graph_dir, format);
	graph.finalize();

	graph.transform_vertices(initialize_vertex_with_hash);
	dc.cout() << "Loading graph in " << t.current_time() << " seconds"
			<< std::endl;
	graphlab::omni_engine<one_hop> engine(dc, graph, exec_type);
	t.start();
	//main iteration
	size_t previous_count = 0;
	size_t diameter = 0;
	for (size_t iter = 0; iter < round; ++iter) {
		engine.signal_all();
		engine.start();

		size_t current_count = 0;
		current_count = graph.map_reduce_vertices<size_t>(absolute_vertex_data_with_hash);
		dc.cout() << iter + 1 << "-th hop: " << current_count << " vertex pairs are reached\n";
		if (iter > 0 && (float) current_count < (float) previous_count * (1.0 + termination_criteria)) {
			diameter = iter;
			dc.cout() << "converge\n";
			//break;
		}
		previous_count = current_count;
	}
 	
	dc.cout() << graph_dir << "\n";
	dc.cout() << "The approximate diameter is " << diameter << "\n";

	dc.cout() << "Finished Running engine in " <<  t.current_time()<< " seconds." << std::endl;
	graphlab::mpi_tools::finalize();

	return EXIT_SUCCESS;
}

