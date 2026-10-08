#include "common_runner.hpp"

int main(int argc, char **argv) {
  return tracer::run_mode(argc, argv,
                          tracer::TracerNodeOptions::be_baseline());
}
