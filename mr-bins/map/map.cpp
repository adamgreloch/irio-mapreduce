#include <iostream>
#include <fstream>
#include <sstream>
#include <map>

void mapFunction(const std::string& inputFilePath, const std::string& outputFilePath) {
    std::ifstream inputFile(inputFilePath);
    std::ofstream outputFile(outputFilePath);

    if (!inputFile.is_open() || !outputFile.is_open()) {
        std::cerr << "Error opening files." << std::endl;
        return;
    }

    std::string line;
    while (std::getline(inputFile, line)) {
        std::istringstream iss(line);
        std::string word;
        while (iss >> word) {
            // Write key-value pair to the output file
            outputFile << word << " 1\n";
        }
    }

    inputFile.close();
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

    mapFunction(inputFilePath, outputFilePath);

    return 0;
}
