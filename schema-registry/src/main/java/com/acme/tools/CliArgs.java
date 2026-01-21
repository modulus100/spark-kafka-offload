package com.acme.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class CliArgs {
    final List<String> configPaths;
    final List<String> configDirs;
    final boolean dryRun;
    final boolean failOnIncompatible;
    final boolean verbose;
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
