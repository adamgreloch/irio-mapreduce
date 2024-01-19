#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <vector>

void reduceFunction(const std::string& inputFilePath, const std::string& outputFilePath) {
    std::ifstream inputFile(inputFilePath);
    std::ofstream outputFile(outputFilePath);

    if (!inputFile.is_open() || !outputFile.is_open()) {
        std::cerr << "Error opening files." << std::endl;
        return;
    }

    std::map<std::string, int> keyValueMap;

    std::string line;
    while (std::getline(inputFile, line)) {
        std::istringstream iss(line);
        std::string key;
        iss >> key;

        int value;
        while (iss >> value) {
            // Accumulate values for the same key
            keyValueMap[key] += value;
        }
    }

    inputFile.close();

    // Write to the output file
    for (const auto& entry : keyValueMap) {
        outputFile << entry.first << " " << entry.second << "\n";
    }

    outputFile.close();
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " -i <input_file_path> -o <output_file_path>" << std::endl;
        return 1;
    }

    std::string inputFilePath;
    std::string outputFilePath;

    for (int i = 1; i < argc; i += 2) {
        if (std::string(argv[i]) == "-i") {
            inputFilePath = argv[i + 1];
        } else if (std::string(argv[i]) == "-o") {
            outputFilePath = argv[i + 1];
        } else {
            std::cerr << "Invalid option: " << argv[i] << std::endl;
            return 1;
        }
    }

    reduceFunction(inputFilePath, outputFilePath);

    return 0;
}
