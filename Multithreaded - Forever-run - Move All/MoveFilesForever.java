import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

public class MoveFilesForever {

    private static boolean isHiddenOrDot(Path p) {
        try {
            Path name = p.getFileName();
            if (name != null && name.toString().startsWith(".")) return true; // dotfile/dir (portable)
            return Files.isHidden(p); // may throw IOException; OS/filesystem-dependent
        } catch (IOException e) {
            // Conservative: if we can't determine, don't move it
            return true;
        }
    }

    private static boolean isMoveCandidate(Path p) {
        try {
            if (!Files.exists(p, NOFOLLOW_LINKS)) return false;
            if (!Files.isRegularFile(p, NOFOLLOW_LINKS)) return false; // excludes directories
            if (isHiddenOrDot(p)) return false; // excludes hidden files
            return true;
        } catch (SecurityException e) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java MoveFilesForever <sourceDir> <targetDir> " +
                    "[--overwrite] [--progressEvery=N] [--threads=N] [--scanEverySeconds=N] [--stableSeconds=N]");
            System.exit(2);
        }

        Path sourceDir = Paths.get(args[0]).toAbsolutePath().normalize();
        Path targetDir = Paths.get(args[1]).toAbsolutePath().normalize();

        boolean overwrite = false;
        long progressEvery = 30_000L;
        int threads = 32;
        long scanEverySeconds = 60;
        long stableSeconds = 30;

        for (int i = 2; i < args.length; i++) {
            String a = args[i];
            if ("--overwrite".equalsIgnoreCase(a)) {
                overwrite = true;
            } else if (a.startsWith("--progressEvery=")) {
                progressEvery = Long.parseLong(a.substring("--progressEvery=".length()));
            } else if (a.startsWith("--threads=")) {
                threads = Integer.parseInt(a.substring("--threads=".length()));
            } else if (a.startsWith("--scanEverySeconds=")) {
                scanEverySeconds = Long.parseLong(a.substring("--scanEverySeconds=".length()));
            } else if (a.startsWith("--stableSeconds=")) {
                stableSeconds = Long.parseLong(a.substring("--stableSeconds=".length()));
            } else {
                System.err.println("Unknown arg: " + a);
                System.exit(2);
            }
        }

        if (threads <= 0) throw new IllegalArgumentException("--threads must be > 0");
        if (scanEverySeconds <= 0) throw new IllegalArgumentException("--scanEverySeconds must be > 0");
        if (stableSeconds < 0) throw new IllegalArgumentException("--stableSeconds must be >= 0");

        if (!Files.isDirectory(sourceDir)) {
            throw new IllegalArgumentException("Source is not a directory: " + sourceDir);
        }
        if (Files.exists(targetDir) && !Files.isDirectory(targetDir)) {
            throw new IllegalArgumentException("Target exists but is not a directory: " + targetDir);
        }
        Files.createDirectories(targetDir);

        final CopyOption[] moveOptions = overwrite
                ? new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
                : new CopyOption[]{};

        final long progressEveryFinal = progressEvery;
        final long stableMillis = TimeUnit.SECONDS.toMillis(stableSeconds);

        final AtomicLong movedCount = new AtomicLong(0);
        final AtomicLong failedCount = new AtomicLong(0);

        final Set<Path> inFlight = ConcurrentHashMap.newKeySet();

        int queueCapacity = Math.max(threads * 1024, 4096);

        ThreadFactory moveTf = r -> {
            Thread t = new Thread(r, "move-worker");
            t.setDaemon(false);
            return t;
        };

        ThreadPoolExecutor moveExec = new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                moveTf,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                Math.min(4, Math.max(1, threads / 8)),
                r -> {
                    Thread t = new Thread(r, "move-scheduler");
                    t.setDaemon(false);
                    return t;
                }
        );

        System.out.println("Starting move-files-forever (TOP-LEVEL FILES ONLY). threads=" + threads +
                ", overwrite=" + overwrite +
                ", progressEvery=" + progressEveryFinal +
                ", scanEverySeconds=" + scanEverySeconds +
                ", stableSeconds=" + stableSeconds +
                ", source=" + sourceDir + ", target=" + targetDir);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown requested...");
            scheduler.shutdownNow();
            moveExec.shutdownNow();
        }, "shutdown-hook"));

        class Mover {
            void scheduleMove(Path file) {
                Path p = file.toAbsolutePath().normalize();

                // Only direct children of sourceDir
                Path parent = p.getParent();
                if (parent == null || !parent.equals(sourceDir)) return;

                if (!isMoveCandidate(p)) return;
                if (!inFlight.add(p)) return;

                scheduler.execute(() -> attemptMove(p, 0));
            }

            void attemptMove(Path src, int attempts) {
                try {
                    if (!isMoveCandidate(src)) {
                        inFlight.remove(src);
                        return;
                    }

                    if (stableMillis > 0) {
                        BasicFileAttributes a1;
                        try {
                            a1 = Files.readAttributes(src, BasicFileAttributes.class, NOFOLLOW_LINKS);
                        } catch (NoSuchFileException e) {
                            inFlight.remove(src);
                            return;
                        }

                        long size1 = a1.size();
                        FileTime mt1 = a1.lastModifiedTime();

                        scheduler.schedule(() -> {
                            try {
                                if (!isMoveCandidate(src)) {
                                    inFlight.remove(src);
                                    return;
                                }
                                BasicFileAttributes a2 = Files.readAttributes(src, BasicFileAttributes.class, NOFOLLOW_LINKS);
                                long size2 = a2.size();
                                FileTime mt2 = a2.lastModifiedTime();

                                if (size1 == size2 && mt1.equals(mt2)) {
                                    submitMove(src);
                                } else {
                                    attemptMove(src, attempts + 1);
                                }
                            } catch (Exception e) {
                                if (attempts < 20) {
                                    scheduler.schedule(() -> attemptMove(src, attempts + 1),
                                            Math.max(1000, stableMillis), TimeUnit.MILLISECONDS);
                                } else {
                                    failedCount.incrementAndGet();
                                    System.err.println("FAILED (stability retries exceeded): " + src + " | " + e);
                                    inFlight.remove(src);
                                }
                            }
                        }, stableMillis, TimeUnit.MILLISECONDS);

                    } else {
                        submitMove(src);
                    }

                } catch (Exception e) {
                    failedCount.incrementAndGet();
                    System.err.println("FAILED (schedule/attempt): " + src + " | " + e);
                    inFlight.remove(src);
                }
            }

            void submitMove(Path src) {
                Path dest = targetDir.resolve(src.getFileName().toString()); // flat move, no folders

                moveExec.execute(() -> {
                    try {
                        if (!isMoveCandidate(src)) return;
                        Files.move(src, dest, moveOptions);

                        long mc = movedCount.incrementAndGet();
                        if (progressEveryFinal > 0 && (mc % progressEveryFinal == 0)) {
                            System.out.println("Moved " + mc + " files...");
                        }
                    } catch (IOException e) {
                        failedCount.incrementAndGet();
                        System.err.println("FAILED to move: " + src + " -> " + dest +
                                " | " + e.getClass().getSimpleName() +
                                (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                    } catch (RuntimeException e) {
                        failedCount.incrementAndGet();
                        System.err.println("FAILED (runtime) to move: " + src + " -> " + dest +
                                " | " + e.getClass().getSimpleName() +
                                (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                    } finally {
                        inFlight.remove(src);
                    }
                });
            }
        }

        Mover mover = new Mover();

        // Periodic scan of only direct children of sourceDir
        Runnable scanOnce = () -> {
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(sourceDir)) {
                for (Path p : ds) {
                    mover.scheduleMove(p);
                }
            } catch (Exception e) {
                System.err.println("Scan failed: " + e);
            }
        };

        // Watch only the source root (not recursive)
        WatchService watchService = FileSystems.getDefault().newWatchService();
        WatchKey rootKey = sourceDir.register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY);

        scheduler.scheduleWithFixedDelay(scanOnce, 0, scanEverySeconds, TimeUnit.SECONDS);

        Thread watcherThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (ClosedWatchServiceException e) {
                    break;
                }

                if (key != rootKey) {
                    key.reset();
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) continue;

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path child = sourceDir.resolve(ev.context()).toAbsolutePath().normalize();
                    mover.scheduleMove(child);
                }

                if (!key.reset()) break;
            }
        }, "watch-service-loop");

        watcherThread.setDaemon(false);
        watcherThread.start();
        watcherThread.join();
    }
}
