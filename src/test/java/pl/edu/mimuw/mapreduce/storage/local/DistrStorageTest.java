package pl.edu.mimuw.mapreduce.storage.local;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DistrStorageTest {

    File createFile(long dirId, long fileId, Path storagePath) {
        Path dirPath = storagePath.resolve(String.valueOf(dirId));
        Path filePath = dirPath.resolve(String.valueOf(fileId));
        return new File(filePath.toString());
    }

    File[] createFiles(int howMany, long inWhichDir, Path storagePath) {
        File[] files = new File[howMany];
        for (int i = 0; i < howMany; i++) {
            File file = createFile(inWhichDir, i, storagePath);
            try {
                Files.write(file.toPath(), "Some file content".getBytes(), StandardOpenOption.CREATE);
                files[i] = file;
            } catch (Exception e) {
                fail("Exception not expected: " + e.getMessage());
            }
        }
        return files;
    }

    void deleteFiles(File[] files) {
        for (File file : files) {
            file.delete();
        }
    }

    @Test
    @DisplayName("getFile() should throw IllegalStateException when file does not exist")
    void getFile() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        long fileId = 1;

        assertThrows(IllegalStateException.class, () -> storage.getFile(String.valueOf(dirId), fileId));
    }

    @Test
    @DisplayName("getFile() should return the file when it exists")
    void getFile2() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        long fileId = 1;
        File dir = new File("1");
        dir.mkdir();

        try {
            File file = new File(dirId + "/" + fileId);
            Files.write(file.toPath(), "Some file content".getBytes(), StandardOpenOption.CREATE);
        } catch (Exception e) {
            dir.delete();
            fail("Exception not expected: " + e.getMessage());
        }

        FileRep fileRep = storage.getFile(String.valueOf(dirId), fileId);
        assertNotNull(fileRep);
        assertEquals(fileId, fileRep.id());

        fileRep.file().delete();
        dir.delete();

    }

    @Test
    @DisplayName("putFile() should copy the content of the temporary file to the storage")
    void putFile() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        long fileId = 1;

        // create folder of name "1"
        File dir = new File("1");
        dir.mkdir();

        File tempFile = new File("tempFile.txt");
        try {
            // Write "abcd" into the temporary file
            Files.write(tempFile.toPath(), "abcd".getBytes(), StandardOpenOption.CREATE);

            // Call putFile method to copy the content to the storage
            storage.putFile(String.valueOf(dirId), fileId, tempFile);

            // Verify if the content of the new file matches the expected content
            FileRep fileRep = storage.getFile(String.valueOf(dirId), fileId);
            assertNotNull(fileRep);
            assertEquals(fileId, fileRep.id());

            // Read the content of the new file
            byte[] fileContent = Files.readAllBytes(fileRep.file().toPath());

            // Verify if the content matches the expected content
            assertArrayEquals("abcd".getBytes(), fileContent);
            fileRep.file().delete();
        } catch (Exception e) {
            fail("Exception not expected: " + e.getMessage());
        } finally {
            tempFile.delete();
            dir.delete();
        }
    }

    @Test
    void getFileCountEmpty() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        File dir = new File("1");
        dir.mkdir();

        long fileCount = storage.getFileCount(String.valueOf(dirId));
        dir.delete();
        assertEquals(0, fileCount);
    }

    @Test
    @DisplayName("getFileCount() should return the number of files in the directory")
    void getFileCount() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;

        // create folder of name "1"
        File dir = new File("1");
        dir.mkdir();

        try {
            File[] files = createFiles(3, dirId, storage.getStoragePath());

            // Call getFileCount method to get the number of files in the directory
            long fileCount = storage.getFileCount(String.valueOf(dirId));

            // Verify if the number of files matches the expected number
            assertEquals(3, fileCount);

            deleteFiles(files);
        } catch (Exception e) {
            fail("Exception not expected: " + e.getMessage());
        } finally {
            dir.delete();
        }
    }

    @Test
    void getSplitsForDir() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        int splits = 3;

        File dir = new File("1");
        dir.mkdir();
        File[] files = createFiles(10, dirId, storage.getStoragePath());

        List<Split> splitList = storage.getSplitsForDir(String.valueOf(dirId), splits);
        assertEquals(splits, splitList.size());
        assertEquals(0, splitList.get(0).getBeg());
        assertEquals(2, splitList.get(0).getEnd());
        assertEquals(3, splitList.get(1).getBeg());
        assertEquals(5, splitList.get(1).getEnd());
        assertEquals(6, splitList.get(2).getBeg());
        assertEquals(9, splitList.get(2).getEnd()); // if dirId length % splits != 0, then the last split should be bigger

        deleteFiles(files);
        dir.delete();
    }

    @Test
    void getSplitIterator() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        long beg = 0;
        long end = 10;
        File dir = new File("1");
        dir.mkdir();
        File[] files = createFiles(20, dirId, storage.getStoragePath()); // create more files than split iterator has
        Split split = new SplitBuilder(beg, end).build();

        Iterator<Path> iterator = storage.getSplitIterator(String.valueOf(dirId), split);
        for (int i = 0; i <= 10; i++) {
            assertTrue(iterator.hasNext());
            Path filePath = iterator.next();
            assertNotNull(filePath);
            assertEquals(String.valueOf(i), filePath.getFileName().toString());
        }

        assertFalse(iterator.hasNext());
        deleteFiles(files);
        dir.delete();
    }

    @Test
    void getDirIterator() {
        DistrStorage storage = new DistrStorage("./");
        long dirId = 1;
        File dir = new File("1");
        dir.mkdir();
        File[] files = createFiles(20, dirId, storage.getStoragePath());

        Iterator<Path> iterator = storage.getDirIterator(String.valueOf(dirId));
        assertNotNull(iterator);
        for (int i = 0; i < 20; i++) {
            assertTrue(iterator.hasNext());
            Path fileRep = iterator.next();
            assertNotNull(fileRep);
            assertEquals(String.valueOf(i), fileRep.getFileName().toString());
        }

        assertFalse(iterator.hasNext());
        deleteFiles(files);
        dir.delete();
    }
}

