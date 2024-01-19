#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

void partitionFunction(int R, const std::string &inputFilePath,
                       const std::string &outputDirectory) {
  std::ifstream inputFile(inputFilePath);

  if (!inputFile.is_open()) {
    std::cerr << "Error opening input file." << std::endl;
    return;
  }

  // Create output files for each partition
  std::vector<std::ofstream> partitionFiles;
  partitionFiles.reserve(R);
  for (int i = 0; i < R; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i; // zero-padded file numbers
    std::string outputPath = outputDirectory + "/" + std::to_string(i) + ".dat";
    partitionFiles.emplace_back(outputPath);
  }

  std::hash<std::string> hashFunction;

  std::string line;
  while (std::getline(inputFile, line)) {
    std::istringstream iss(line);
    std::string key, value;
    iss >> key >> value;

    // Use hash function to determine the partition
    size_t hashValue = hashFunction(key);
    int partitionNumber = static_cast<int>(hashValue % R);

    // Write key-value pair to the corresponding partition file
    partitionFiles[partitionNumber] << key << " " << value << "\n";
  }

  inputFile.close();

  // Close all partition files
  for (auto &partitionFile : partitionFiles) {
    partitionFile.close();
  }
}

int main(int argc, char *argv[]) {
  if (argc != 7) {
    std::cerr << "Usage: " << argv[0]
              << " -R <R-param> -i <input_file_path> -o <output_directory>"
              << std::endl;
    return 1;
  }

  int R = std::stoi(argv[2]);
  std::string inputFilePath;
  std::string outputDirectory;

  for (int i = 3; i < argc; i += 2) {
    if (std::string(argv[i]) == "-i") {
      inputFilePath = argv[i + 1];
    } else if (std::string(argv[i]) == "-o") {
      outputDirectory = argv[i + 1];
    } else {
      std::cerr << "Invalid option: " << argv[i] << std::endl;
      return 1;
    }
  }

  partitionFunction(R, inputFilePath, outputDirectory);

  return 0;
}
