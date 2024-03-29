package pl.edu.mimuw.mapreduce.storage.local;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import pl.edu.mimuw.mapreduce.Utils;
import pl.edu.mimuw.mapreduce.storage.FileRep;
import pl.edu.mimuw.mapreduce.storage.SplitBuilder;
import pl.edu.mimuw.proto.common.Split;

import java.io.File;
import java.io.IOException;
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
    void getFile() throws IOException {
        String dirId = "1";
        long fileId = 1;

        Files.createDirectories(tmpDirPath.resolve(dirId));

        assertThrows(IllegalStateException.class, () -> storage.getFile(String.valueOf(dirId), fileId));
    }

    private static Path tmpDirPath;
    private static DistrStorage storage;

    @BeforeAll
    @DisplayName("Create temp dir and create storage instance")
    static void setupStorage() throws IOException {
        tmpDirPath = Files.createTempDirectory("distr_storage_test").toAbsolutePath();

        storage = new DistrStorage(tmpDirPath.toString());
    }

    @AfterAll
    @DisplayName("Remove temp dir")
    static void cleanup() {
        Utils.removeDirRecursively(tmpDirPath.toFile());
    }

    @Test
    @DisplayName("getFile() should return the file when it exists")
    void getFile2() throws IOException {
        String dirId = "1";
        long fileId = 1;

        Files.createDirectories(tmpDirPath.resolve(dirId));

        try {
            File file = new File(tmpDirPath.resolve(dirId).resolve(String.valueOf(fileId)).toString());
            Files.write(file.toPath(), "Some file content".getBytes(), StandardOpenOption.CREATE);
        } catch (Exception e) {
            fail("Exception not expected: " + e);
        }

        FileRep fileRep = storage.getFile(dirId, fileId);
        assertNotNull(fileRep);
        assertEquals(fileId, fileRep.id());
    }

    @Test
    @DisplayName("putFile() should copy the content of the temporary file to the storage")
    void putFile() throws IOException {
        String dirId = "1";
        long fileId = 1;

        Files.createDirectories(tmpDirPath.resolve(dirId));

        File tempFile = new File(tmpDirPath.resolve(dirId).resolve(String.valueOf(fileId)).toString());
        try {
            // Write "abcd" into the temporary file
            Files.write(tempFile.toPath(), "abcd".getBytes(), StandardOpenOption.CREATE);

            // Call putFile method to copy the content to the storage
            storage.putFile(dirId, fileId, tempFile);

            // Verify if the content of the new file matches the expected content
            FileRep fileRep = storage.getFile(dirId, fileId);
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
            Utils.removeDirRecursively(tmpDirPath.resolve(dirId).toFile());
        }
    }

    @Test
    @DisplayName("getFileCount() should return 0 when directory is empty")
    void getFileCountEmpty() throws IOException, IllegalStateException {
        String dirId = "1";
        Files.createDirectories(tmpDirPath.resolve(dirId));

        long fileCount = storage.getFileCount(dirId);
        Utils.removeDirRecursively(tmpDirPath.resolve(dirId).toFile());
        assertEquals(0, fileCount);
    }

    @Test
    @DisplayName("getFileCount() should return IllegalStateException when directory does not exist")
    void getFileCountDirDoesNotExist() {
        String dirId = "99";
        assertThrows(IllegalStateException.class, () -> storage.getFileCount(dirId));
    }

    @Test
    @DisplayName("getFileCount() should return the number of files in the directory")
    void getFileCount() throws IOException {
        String dirId = "1";

        Files.createDirectories(tmpDirPath.resolve(dirId));

        try {
            File[] files = createFiles(3, Long.parseLong(dirId), storage.getStoragePath());

            // Call getFileCount method to get the number of files in the directory
            long fileCount = storage.getFileCount(String.valueOf(dirId));

            // Verify if the number of files matches the expected number
            assertEquals(3, fileCount);

            deleteFiles(files);
        } catch (Exception e) {
            fail("Exception not expected: " + e.getMessage());
        } finally {
            Utils.removeDirRecursively(tmpDirPath.resolve(dirId).toFile());
        }
    }

    @Test
    @DisplayName("getSplitsForDir() should return an empty list when directory is empty")
    void getSplitsForDirEmpty() throws IOException {
        String dirId = "1";
        int splits = 0;

        Files.createDirectories(tmpDirPath.resolve(dirId));
        File dir = new File("1");
        dir.mkdir();

        List<Split> splitList = storage.getSplitsForDir(dirId, splits);

        assertEquals(0, splitList.size());
        dir.delete();
    }

    @Test
    @DisplayName("getSplitsForDir() should return correctly sized splits if dirId length % splits == 0")
    void getSplitsForDir2() throws IOException {
        String dirId = "2";
        int splits = 3;

        Files.createDirectories(tmpDirPath.resolve(dirId));

        File dir = new File("2");
        dir.mkdir();
        File[] files = createFiles(9, Long.parseLong(dirId), storage.getStoragePath());

        List<Split> splitList = storage.getSplitsForDir(dirId, splits);
        assertEquals(splits, splitList.size());
        assertEquals(0, splitList.get(0).getBeg());
        assertEquals(2, splitList.get(0).getEnd());
        assertEquals(3, splitList.get(1).getBeg());
        assertEquals(5, splitList.get(1).getEnd());
        assertEquals(6, splitList.get(2).getBeg());
        assertEquals(8, splitList.get(2).getEnd());

        deleteFiles(files);
        dir.delete();
    }

    @Test
    @DisplayName("getSplitsForDir() should return correctly sized splits even if dirId length % splits != 0")
    void getSplitsForDir() throws IOException {
        String dirId = "3";
        int splits = 3;

        Files.createDirectories(tmpDirPath.resolve(dirId));

        File dir = new File("3");
        dir.mkdir();
        File[] files = createFiles(10, Long.parseLong(dirId), storage.getStoragePath());

        List<Split> splitList = storage.getSplitsForDir(dirId, splits);
        assertEquals(splits, splitList.size());
        assertEquals(0, splitList.get(0).getBeg());
        assertEquals(2, splitList.get(0).getEnd());
        assertEquals(3, splitList.get(1).getBeg());
        assertEquals(5, splitList.get(1).getEnd());
        assertEquals(6, splitList.get(2).getBeg());
        assertEquals(9,
                splitList.get(2).getEnd()); // if dirId length % splits != 0, then the last split should be bigger

        deleteFiles(files);
        dir.delete();
    }

    @Test
    @DisplayName("getSplitIterator() should return a correct split iterator")
    void getSplitIterator() throws IOException {
        String dirId = "1";
        long beg = 0;
        long end = 10;
        Files.createDirectories(tmpDirPath.resolve(dirId));
        File dir = new File("1");
        dir.mkdir();
        File[] files = createFiles(20, Long.parseLong(dirId),
                storage.getStoragePath()); // create more files than split iterator has
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
    @DisplayName("getSplitIterator() should return a correct dir iterator")
    void getDirIterator() throws IOException {
        String dirId = "1";
        File dir = new File("1");
        Files.createDirectories(tmpDirPath.resolve(dirId));
        dir.mkdir();
        File[] files = createFiles(20, Long.parseLong(dirId), storage.getStoragePath());

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

    @Test
    @DisplayName("saveState() should correctly save a state file")
    void saveState() {
        String podId = "testPod";
        String state = "Sample state content";

        storage.saveState(podId, state);

        try {
            String savedState = Files.readString(storage.getStoragePath().resolve("STATE_DIR").resolve(podId));

            assertEquals(state, savedState);
        } catch (java.io.IOException e) {
            fail("Exception not expected: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("retrieveState() should correctly retrieve a state file")
    void retrieveState() {
        String podId = "testPod";
        String state = "Sample state content";

        storage.saveState(podId, state);
        String retrievedState = storage.retrieveState(podId);
        assertEquals(state, retrievedState);
    }

    @Test
    @DisplayName("retrieveState() should correctly remove duplicate reduce output files")
    void removeReduceDuplicates() throws IOException {
        String dirId = "1";
        storage.createDir(dirId);

        Path filePath1 = tmpDirPath.resolve(dirId).resolve("1_R_pod1");
        Path filePath2 = tmpDirPath.resolve(dirId).resolve("1_R_pod2");
        Path filePath3 = tmpDirPath.resolve(dirId).resolve("1_R_pod1_duplicate");
        Path filePath4 = tmpDirPath.resolve(dirId).resolve("2_R_pod1");

        Files.createDirectories(tmpDirPath);
        Files.createFile(filePath1);
        Files.createFile(filePath2);
        Files.createFile(filePath3);
        Files.createFile(filePath4);

        String finalDestDirId = "2";

        storage.moveUniqueReduceResultsToDestDir(dirId, finalDestDirId);

        var uniqueFile1 = tmpDirPath.resolve(finalDestDirId).resolve("1");
        var uniqueFile2 = tmpDirPath.resolve(finalDestDirId).resolve("2");

        filePath1 = tmpDirPath.resolve(finalDestDirId).resolve("1_R_pod1");
        filePath2 = tmpDirPath.resolve(finalDestDirId).resolve("1_R_pod2");
        filePath3 = tmpDirPath.resolve(finalDestDirId).resolve("1_R_pod1_duplicate");
        filePath4 = tmpDirPath.resolve(finalDestDirId).resolve("2_R_pod1");

        assertFalse(Files.exists(filePath1));
        assertFalse(Files.exists(filePath2));
        assertFalse(Files.exists(filePath3));
        assertFalse(Files.exists(filePath4));
        assertTrue(Files.exists(uniqueFile1));
        assertTrue(Files.exists(uniqueFile2));
    }

    @Test
    @DisplayName("retrieveState() should not mutate a directory when all files are unique")
    void removeReduceDuplicatesNoDuplicates() throws IOException {
        String dirId = "1";
        storage.createDir(dirId);

        Path filePath1 = tmpDirPath.resolve(dirId).resolve("1_R_pod1");
        Path filePath2 = tmpDirPath.resolve(dirId).resolve("2_R_pod1");
        Path filePath3 = tmpDirPath.resolve(dirId).resolve("3_R_pod1");

        Files.createDirectories(tmpDirPath);
        Files.createFile(filePath1);
        Files.createFile(filePath2);
        Files.createFile(filePath3);

        storage.removeReduceDuplicates(dirId);

        assertTrue(Files.exists(filePath1));
        assertTrue(Files.exists(filePath2));
        assertTrue(Files.exists(filePath3));
    }
}

