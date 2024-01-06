package pl.edu.mimuw.mapreduce.storage.local;

import pl.edu.mimuw.mapreduce.storage.FileRep;

import java.io.File;

public record LocalFileRep(File file, long id) implements FileRep {
}
