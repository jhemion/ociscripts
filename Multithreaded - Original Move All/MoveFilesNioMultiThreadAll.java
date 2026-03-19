import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MoveFilesNioMultiThreadAll {

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: java MoveFilesNioMultiThreadAll <sourceDir> <targetDir> " +
                    "[--overwrite] [--progressEvery=N] [--deleteEmptyDirs] [--threads=N]");
            System.exit(2);
        }

        Path sourceDir = Paths.get(args[0]).toAbsolutePath().normalize();
        Path targetDir = Paths.get(args[1]).toAbsolutePath().normalize();

        boolean overwrite = false;
        long progressEvery = 30_000L;
        boolean deleteEmptyDirs = false;
        int threads = 32;

        for (int i = 2; i < args.length; i++) {
            String a = args[i];
            if ("--overwrite".equalsIgnoreCase(a)) {
                overwrite = true;
            } else if (a.startsWith("--progressEvery=")) {
                progressEvery = Long.parseLong(a.substring("--progressEvery=".length()));
            } else if ("--deleteEmptyDirs".equalsIgnoreCase(a)) {
                deleteEmptyDirs = true;
            } else if (a.startsWith("--threads=")) {
                threads = Integer.parseInt(a.substring("--threads=".length()));
            } else {
                System.err.println("Unknown arg: " + a);
                System.exit(2);
            }
        }

        if (threads <= 0) throw new IllegalArgumentException("--threads must be > 0");

        if (!Files.isDirectory(sourceDir)) {
            throw new IllegalArgumentException("Source is not a directory: " + sourceDir);
        }
        if (Files.exists(targetDir) && !Files.isDirectory(targetDir)) {
            throw new IllegalArgumentException("Target exists but is not a directory: " + targetDir);
        }
        Files.createDirectories(targetDir);

        final boolean targetInsideSource = targetDir.startsWith(sourceDir);
        final CopyOption[] moveOptions = overwrite
                ? new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
                : new CopyOption[]{};

        final long progressEveryFinal = progressEvery;
        final boolean deleteEmptyDirsFinal = deleteEmptyDirs;

        final AtomicLong movedCount = new AtomicLong(0);
        final AtomicLong failedCount = new AtomicLong(0);

        // Track directories so we can delete empty ones after all moves finish (deepest-first).
        final ConcurrentLinkedQueue<Path> dirsSeen = new ConcurrentLinkedQueue<>();

        // Bounded queue for backpressure (prevents unbounded memory growth on huge trees)
        int queueCapacity = Math.max(threads * 1024, 4096);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "move-worker");
            t.setDaemon(false);
            return t;
        };

        ThreadPoolExecutor exec = new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                tf,
                new ThreadPoolExecutor.CallerRunsPolicy() // backpressure: walker thread helps when saturated
        );

        System.out.println("Starting move-all (MULTI-THREADED). threads=" + threads +
                ", progressEvery=" + progressEveryFinal +
                ", overwrite=" + overwrite +
                ", deleteEmptyDirs=" + deleteEmptyDirsFinal +
                ", source=" + sourceDir + ", target=" + targetDir);

        try {
            Files.walkFileTree(sourceDir, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE,
                    new SimpleFileVisitor<>() {

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            Path nd = dir.toAbsolutePath().normalize();

                            // Avoid walking into the target if it's under the source
                            if (targetInsideSource && nd.startsWith(targetDir)) {
                                return FileVisitResult.SKIP_SUBTREE;
                            }

                            dirsSeen.add(nd);

                            Path rel = sourceDir.relativize(nd);
                            Path destDir = targetDir.resolve(rel);
                            Files.createDirectories(destDir);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            Path nf = file.toAbsolutePath().normalize();

                            // If target is inside source, ignore anything in target
                            if (targetInsideSource && nf.startsWith(targetDir)) {
                                return FileVisitResult.CONTINUE;
                            }

                            Path rel = sourceDir.relativize(nf);
                            Path dest = targetDir.resolve(rel);

                            exec.execute(() -> {
                                try {
                                    Path parent = dest.getParent();
                                    if (parent != null) Files.createDirectories(parent);

                                    Files.move(nf, dest, moveOptions);

                                    long mc = movedCount.incrementAndGet();
                                    if (progressEveryFinal > 0 && (mc % progressEveryFinal == 0)) {
                                        System.out.println("Moved " + mc + " files...");
                                    }
                                } catch (IOException e) {
                                    failedCount.incrementAndGet();
                                    System.err.println(
                                            "FAILED to move: " + nf + " -> " + dest +
                                                    " | " + e.getClass().getSimpleName() +
                                                    (e.getMessage() != null ? (": " + e.getMessage()) : "")
                                    );
                                } catch (RuntimeException e) {
                                    failedCount.incrementAndGet();
                                    System.err.println("FAILED (runtime) to move: " + nf + " -> " + dest +
                                            " | " + e.getClass().getSimpleName() +
                                            (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                                }
                            });

                            return FileVisitResult.CONTINUE;
                        }
                    });

        } finally {
            exec.shutdown();
            boolean finished = exec.awaitTermination(365, TimeUnit.DAYS);
            if (!finished) {
                System.err.println("Executor did not terminate in time.");
                System.exit(1);
            }
        }

        if (deleteEmptyDirsFinal) {
            // Delete deepest-first so parents become empty after children are removed.
            // De-dup and sort by depth descending.
            ArrayList<Path> dirs = new ArrayList<>(new HashSet<>(dirsSeen));
            dirs.sort((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount()));

            for (Path d : dirs) {
                Path nd = d.toAbsolutePath().normalize();

                if (nd.equals(sourceDir)) continue; // never delete source root
                if (targetInsideSource && nd.startsWith(targetDir)) continue;

                try {
                    Files.delete(nd); // only deletes if empty
                } catch (IOException ignored) {
                    // not empty or can't delete; ignore
                }
            }
        }

        long m = movedCount.get();
        long f = failedCount.get();
        System.out.println("Done. Moved=" + m + ", Failed=" + f);
        if (f > 0) System.exit(1);
    }
}