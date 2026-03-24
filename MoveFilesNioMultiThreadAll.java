import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MoveFilesNioMultiThreadAll {

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: java MoveFilesNioMultiThreadAll <sourceDir> <targetDir> " +
                    "[--overwrite] [--progressEvery=N] [--deleteEmptyDirs] [--threads=N] [--useLfsFind]");
            System.exit(2);
        }

        Path sourceDir = Paths.get(args[0]).toAbsolutePath().normalize();
        Path targetDir = Paths.get(args[1]).toAbsolutePath().normalize();

        boolean overwrite = false;
        long progressEvery = 30_000L;
        boolean deleteEmptyDirs = false;
        int threads = 32;
        boolean useLfsFind = false;

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
            } else if ("--useLfsFind".equalsIgnoreCase(a)) {
                useLfsFind = true;
            } else {
                System.err.println("Unknown arg: " + a);
                System.exit(2);
            }
        }

        if (threads <= 0) throw new IllegalArgumentException("--threads must be > 0");
        if (!Files.isDirectory(sourceDir)) throw new IllegalArgumentException("Source is not a directory: " + sourceDir);
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
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        System.out.println("Starting move (MULTI-THREADED, OS listing, maxDepth=1). threads=" + threads +
                ", progressEvery=" + progressEveryFinal +
                ", overwrite=" + overwrite +
                ", deleteEmptyDirs=" + deleteEmptyDirsFinal +
                ", useLfsFind=" + useLfsFind +
                ", source=" + sourceDir + ", target=" + targetDir);

        // SECURITY NOTE: ProcessBuilder executes OS commands. Keep sourceDir trusted; avoid concatenating
        // untrusted strings into a shell command. This implementation passes args as a list.

        List<Path> files;
        try {
            if (useLfsFind) {
                files = listTopLevelFilesViaGitLfsFind(sourceDir);
            } else {
                files = listTopLevelFilesViaOs(sourceDir);
            }
        } catch (Exception e) {
            System.err.println("OS-based listing failed (" + e.getClass().getSimpleName() + ": " + e.getMessage() +
                    "). Falling back to Files.list().");
            files = listTopLevelFilesViaJava(sourceDir);
        }

        try {
            for (Path f : files) {
                Path nf = f.toAbsolutePath().normalize();

                // Enforce maxDepth=1 and safety checks
                if (targetInsideSource && nf.startsWith(targetDir)) continue;
                if (!nf.getParent().equals(sourceDir)) continue;
                if (!Files.isRegularFile(nf)) continue;

                Path rel = sourceDir.relativize(nf); // should be just "filename"
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
                        System.err.println("FAILED to move: " + nf + " -> " + dest +
                                " | " + e.getClass().getSimpleName() +
                                (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                    } catch (RuntimeException e) {
                        failedCount.incrementAndGet();
                        System.err.println("FAILED (runtime) to move: " + nf + " -> " + dest +
                                " | " + e.getClass().getSimpleName() +
                                (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                    }
                });
            }
        } finally {
            exec.shutdown();
            boolean finished = exec.awaitTermination(365, TimeUnit.DAYS);
            if (!finished) {
                System.err.println("Executor did not terminate in time.");
                System.exit(1);
            }
        }

        if (deleteEmptyDirsFinal) {
            // With maxDepth=1 moves, only attempt to delete empty immediate subdirectories.
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(sourceDir)) {
                for (Path p : ds) {
                    Path np = p.toAbsolutePath().normalize();
                    if (targetInsideSource && np.startsWith(targetDir)) continue;
                    if (Files.isDirectory(np)) {
                        try {
                            Files.delete(np); // only deletes if empty
                        } catch (IOException ignored) {
                            // not empty or can't delete; ignore
                        }
                    }
                }
            }
        }

        long m = movedCount.get();
        long f = failedCount.get();
        System.out.println("Done. Moved=" + m + ", Failed=" + f);
        if (f > 0) System.exit(1);
    }

    private static List<Path> listTopLevelFilesViaJava(Path sourceDir) throws IOException {
        List<Path> out = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(sourceDir)) {
            for (Path p : ds) {
                if (Files.isRegularFile(p)) out.add(p);
            }
        }
        return out;
    }

    /**
     * Uses Git LFS to list files. This only returns LFS-tracked files.
     *
     * We run from sourceDir and parse output as relative paths.
     * We then convert to absolute Paths under sourceDir and enforce maxDepth=1 in caller.
     *
     * Command executed (conceptually): git lfs find .
     */
    private static List<Path> listTopLevelFilesViaGitLfsFind(Path sourceDir) throws IOException, InterruptedException {
        // Prefer explicit args (no shell). Also set working directory.
        ProcessBuilder pb = new ProcessBuilder("git", "lfs", "find", ".");
        pb.directory(sourceDir.toFile());
        pb.redirectErrorStream(true);

        Process proc = pb.start();
        String output = readAllToString(proc.getInputStream());
        int rc = proc.waitFor();
        if (rc != 0) {
            throw new IOException("git lfs find failed. exitCode=" + rc + ", output=" + output);
        }

        // git lfs find output format can vary; commonly lines contain the path.
        // We'll conservatively extract the last whitespace-separated token if it looks like a path,
        // otherwise take the whole line.
        List<Path> out = new ArrayList<>();
        for (String line : output.split("\\R")) {
            String s = line.trim();
            if (s.isEmpty()) continue;

            // Heuristic: take last token as candidate path
            String[] toks = s.split("\\s+");
            String candidate = toks[toks.length - 1].trim();

            // If candidate is "." or looks weird, fall back to the entire line
            String rel = candidate.equals(".") ? s : candidate;

            // Normalize common prefixes
            if (rel.startsWith("./")) rel = rel.substring(2);

            Path p = sourceDir.resolve(rel).normalize();
            out.add(p);
        }
        return out;
    }

    /**
     * OS-based non-recursive listing of regular files (maxDepth=1).
     * - Windows: PowerShell Get-ChildItem -File (non-recursive)
     * - Unix/macOS: find -maxdepth 1 -type f -print0
     */
    private static List<Path> listTopLevelFilesViaOs(Path sourceDir) throws IOException, InterruptedException {
        boolean windows = System.getProperty("os.name").toLowerCase().contains("win");
        return windows ? listTopLevelFilesViaPowerShell(sourceDir) : listTopLevelFilesViaFind(sourceDir);
    }

    private static List<Path> listTopLevelFilesViaPowerShell(Path sourceDir) throws IOException, InterruptedException {
        String ps = ""
                + "$p = '" + escapeForPowerShellSingleQuoted(sourceDir.toString()) + "'; "
                + "Get-ChildItem -LiteralPath $p -File | ForEach-Object { $_.FullName }";

        ProcessBuilder pb = new ProcessBuilder("powershell", "-NoProfile", "-NonInteractive", "-Command", ps);
        pb.redirectErrorStream(true);

        Process proc = pb.start();
        String output = readAllToString(proc.getInputStream());
        int rc = proc.waitFor();
        if (rc != 0) {
            throw new IOException("PowerShell listing failed. exitCode=" + rc + ", output=" + output);
        }

        List<Path> out = new ArrayList<>();
        for (String line : output.split("\\R")) {
            String s = line.trim();
            if (!s.isEmpty()) out.add(Paths.get(s));
        }
        return out;
    }

    private static List<Path> listTopLevelFilesViaFind(Path sourceDir) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(
                "find",
                sourceDir.toString(),
                "-maxdepth", "1",
                "-type", "f",
                "-print0"
        );
        pb.redirectErrorStream(true);

        Process proc = pb.start();
        byte[] bytes = readAllBytes(proc.getInputStream());
        int rc = proc.waitFor();
        if (rc != 0) {
            String out = new String(bytes, StandardCharsets.UTF_8);
            throw new IOException("find listing failed. exitCode=" + rc + ", output=" + out);
        }

        List<Path> out = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0) {
                if (i > start) {
                    String s = new String(bytes, start, i - start, StandardCharsets.UTF_8);
                    if (!s.isBlank()) out.add(Paths.get(s));
                }
                start = i + 1;
            }
        }
        return out;
    }

    private static byte[] readAllBytes(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int r;
        while ((r = in.read(buf)) != -1) {
            baos.write(buf, 0, r);
        }
        return baos.toByteArray();
    }

    private static String readAllToString(InputStream in) throws IOException {
        return new String(readAllBytes(in), StandardCharsets.UTF_8);
    }

    private static String escapeForPowerShellSingleQuoted(String s) {
        return s.replace("'", "''");
    }
}
