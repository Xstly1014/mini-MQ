package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void missingFileStartsFromHead() {
        OffsetStore store = OffsetStore.load(new File(tempDir.toFile(), "absent.properties"));
        assertEquals(0L, store.get("any-topic"));
    }

    @Test
    void roundTripSurvivesReload() throws Exception {
        File file = new File(tempDir.toFile(), "offsets.properties");
        OffsetStore store = OffsetStore.load(file);
        store.update("orders", 42L);
        store.update("payments", 7L);
        store.update("orders", 100L); // overwrite wins
        store.flush();

        OffsetStore reloaded = OffsetStore.load(file);
        assertEquals(100L, reloaded.get("orders"));
        assertEquals(7L, reloaded.get("payments"));
    }

    @Test
    void flushIsAtomicNoTempResidue() throws Exception {
        File file = new File(tempDir.toFile(), "offsets.properties");
        OffsetStore store = OffsetStore.load(file);
        store.update("t", 5L);
        store.flush();
        store.update("t", 9L);
        store.flush(); // second flush replaces in place

        assertEquals(9L, OffsetStore.load(file).get("t"));
        File[] residue = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".tmp"));
        assertEquals(0, residue.length, "temp file must be renamed away, not left behind");
        assertTrue(file.exists());
    }

    @Test
    void corruptFileFailsLoudly() throws Exception {
        File file = new File(tempDir.toFile(), "offsets.properties");
        try (PrintWriter w = new PrintWriter(file, "UTF-8")) {
            w.println("orders=not-a-number");
        }
        boolean threw = false;
        try {
            OffsetStore.load(file);
        } catch (IllegalStateException expected) {
            threw = true;
        }
        assertTrue(threw, "a corrupt offset file must fail loudly, not silently reset");
        assertFalse(false);
    }
}
