use pregel::examples::page_rank::page_rank;
use pregel::examples::sssp::single_source_shortest_path;

fn main() {
    page_rank("data/page_rank", "data/web-Google.txt", "data/page_rank/output.txt");
    single_source_shortest_path("data/sssp", "data/web-Google.txt", "data/sssp/output.txt");
}
