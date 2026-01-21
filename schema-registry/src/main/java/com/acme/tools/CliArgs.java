package com.acme.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Parsed CLI arguments for {@link SchemaRegister}.
 */
final class CliArgs {

    /**
     * Explicit config file paths provided via {@code --config <path>}.
     */
    final List<String> configPaths;

    /**
     * Directories to scan recursively for {@code schema-registry.yml} via {@code --config-dir <dir>}.
     */
    final List<String> configDirs;

    /**
     * If true, do not register schemas or update compatibility in Schema Registry.
     *
     * The tool may still resolve schemas and perform compatibility checks to show what would happen.
     */
    final boolean dryRun;

    /**
     * If true, stop processing an entry when the candidate schema is incompatible with an existing subject.
     */
    final boolean failOnIncompatible;

    /**
     * If true, emit more detailed logs about what the tool is doing.
     */
    final boolean verbose;

    /**
     * Schema Registry base URL provided via {@code --schema-registry-url <url>}.
     */
    final Optional<String> schemaRegistryUrlOverride;

    private CliArgs(
            List<String> configPaths,
            List<String> configDirs,
            boolean dryRun,
            boolean failOnIncompatible,
            boolean verbose,
            Optional<String> schemaRegistryUrlOverride
    ) {
        this.configPaths = configPaths;
        this.configDirs = configDirs;
        this.dryRun = dryRun;
        this.failOnIncompatible = failOnIncompatible;
        this.verbose = verbose;
        this.schemaRegistryUrlOverride = schemaRegistryUrlOverride;
    }

    static CliArgs parse(String[] args) {
        List<String> configPaths = new ArrayList<>();
        List<String> configDirs = new ArrayList<>();
        boolean dryRun = false;
        boolean failOnIncompatible = true;
        boolean verbose = false;
        String schemaRegistryUrlOverride = null;

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--config" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--config requires a value");
                    }
                    configPaths.add(args[++i]);
                }
                case "--config-dir" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--config-dir requires a value");
                    }
                    configDirs.add(args[++i]);
                }
                case "--schema-registry-url" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--schema-registry-url requires a value");
                    }
                    schemaRegistryUrlOverride = args[++i];
                }
                case "--dry-run" -> dryRun = true;
                case "--no-fail-on-incompatible" -> failOnIncompatible = false;
                case "--verbose" -> verbose = true;
                default -> throw new IllegalArgumentException("Unknown arg: " + a);
            }
        }

        boolean hasAnyConfigPath = configPaths.stream().anyMatch(p -> p != null && !p.isBlank());
        boolean hasAnyConfigDir = configDirs.stream().anyMatch(d -> d != null && !d.isBlank());
        if (!hasAnyConfigPath && !hasAnyConfigDir) {
            throw new IllegalArgumentException("Provide at least one of --config <path> or --config-dir <dir>");
        }

        if (schemaRegistryUrlOverride == null || schemaRegistryUrlOverride.isBlank()) {
            throw new IllegalArgumentException("Missing required --schema-registry-url <url>");
        }

        return new CliArgs(
                List.copyOf(configPaths),
                List.copyOf(configDirs),
                dryRun,
                failOnIncompatible,
                verbose,
                Optional.ofNullable(schemaRegistryUrlOverride)
        );
    }
}
