import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

public class MoveFilesNioSingleThreadAll {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java MoveFilesNioSingleThreadAll <sourceDir> <targetDir> " +
                    "[--overwrite] [--progressEvery=N] [--deleteEmptyDirs]");
            System.exit(2);
        }

        Path sourceDir = Paths.get(args[0]).toAbsolutePath().normalize();
        Path targetDir = Paths.get(args[1]).toAbsolutePath().normalize();

        boolean overwrite = false;
        long progressEvery = 30_000L;
        boolean deleteEmptyDirs = false;

        for (int i = 2; i < args.length; i++) {
            String a = args[i];
            if ("--overwrite".equalsIgnoreCase(a)) {
                overwrite = true;
            } else if (a.startsWith("--progressEvery=")) {
                progressEvery = Long.parseLong(a.substring("--progressEvery=".length()));
            } else if ("--deleteEmptyDirs".equalsIgnoreCase(a)) {
                deleteEmptyDirs = true;
            } else {
                System.err.println("Unknown arg: " + a);
                System.exit(2);
            }
        }

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

        final long[] movedCount = {0};
        final long[] failedCount = {0};

        System.out.println("Starting move-all. progressEvery=" + progressEveryFinal +
                ", overwrite=" + overwrite +
                ", deleteEmptyDirs=" + deleteEmptyDirsFinal +
                ", source=" + sourceDir + ", target=" + targetDir);

        Files.walkFileTree(sourceDir, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        Path nd = dir.toAbsolutePath().normalize();

                        // Avoid walking into the target if it's under the source
                        if (targetInsideSource && nd.startsWith(targetDir)) {
                            return FileVisitResult.SKIP_SUBTREE;
                        }

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

                        try {
                            Files.createDirectories(dest.getParent());
                            Files.move(nf, dest, moveOptions);
                            movedCount[0]++;

                            if (progressEveryFinal > 0 && (movedCount[0] % progressEveryFinal == 0)) {
                                System.out.println("Moved " + movedCount[0] + " files...");
                            }
                        } catch (IOException e) {
                            failedCount[0]++;
                            System.err.println(
                                    "FAILED to move: " + nf + " -> " + dest +
                                    " | " + e.getClass().getSimpleName() +
                                    (e.getMessage() != null ? (": " + e.getMessage()) : "")
                            );
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                        if (!deleteEmptyDirsFinal) return FileVisitResult.CONTINUE;

                        Path nd = dir.toAbsolutePath().normalize();

                        // Never delete the source root itself
                        if (nd.equals(sourceDir)) return FileVisitResult.CONTINUE;

                        // Don't try to delete anything in/under target (if target is inside source)
                        if (targetInsideSource && nd.startsWith(targetDir)) return FileVisitResult.CONTINUE;

                        try {
                            Files.delete(nd); // succeeds only if empty
                        } catch (IOException ignored) {
                            // directory not empty or cannot delete; ignore
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

        System.out.println("Done. Moved=" + movedCount[0] + ", Failed=" + failedCount[0]);
        if (failedCount[0] > 0) System.exit(1);
    }
}

