/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.cli;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASSPATH_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JARDIR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PARALLELISM_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SHUTDOWN_IF_ATTACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.YARN_DETACHED_OPTION;
import static org.apache.flink.client.cli.ProgramOptionsUtils.containsPythonDependencyOptions;
import static org.apache.flink.client.cli.ProgramOptionsUtils.createPythonProgramOptions;
import static org.apache.flink.client.cli.ProgramOptionsUtils.isPythonEntryPoint;

/** Base class for command line options that refer to a JAR file program. */
public class ProgramOptions extends CommandLineOptions {

    private String jarFilePath;

    protected String entryPointClass;

    private final List<URL> classpaths;

    private final String[] programArgs;

    private final int parallelism;

    private final boolean detachedMode;

    private final boolean shutdownOnAttachedExit;

    private final SavepointRestoreSettings savepointSettings;

    protected ProgramOptions(CommandLine line) throws CliArgsException {
        super(line);

        this.entryPointClass =
                line.hasOption(CLASS_OPTION.getOpt())
                        ? line.getOptionValue(CLASS_OPTION.getOpt())
                        : null;

        this.jarFilePath =
                line.hasOption(JAR_OPTION.getOpt())
                        ? line.getOptionValue(JAR_OPTION.getOpt())
                        : null;

        this.programArgs = extractProgramArgs(line);

        List<URL> classpaths = new ArrayList<URL>();
        if (line.hasOption(CLASSPATH_OPTION.getOpt())) {
            for (String path : line.getOptionValues(CLASSPATH_OPTION.getOpt())) {
                try {
                    classpaths.add(new URL(path));
                } catch (MalformedURLException e) {
                    throw new CliArgsException("Bad syntax for classpath: " + path);
                }
            }
        }
        // load jardir all jar.
        if (line.hasOption(JARDIR_OPTION.getOpt())) {
            for (String path : line.getOptionValues(JARDIR_OPTION.getOpt())) {
                List<URL> jarFiles = null;
                try {
                    jarFiles = loadAllJarFromPathURl(path);
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                    throw new CliArgsException("Bad syntax for classpath: " + path);
                }
                // classpaths.add(new URL(path));
                classpaths.addAll(jarFiles);
            }
        }
        this.classpaths = classpaths;

        if (line.hasOption(PARALLELISM_OPTION.getOpt())) {
            String parString = line.getOptionValue(PARALLELISM_OPTION.getOpt());
            try {
                parallelism = Integer.parseInt(parString);
                if (parallelism <= 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new CliArgsException(
                        "The parallelism must be a positive number: " + parString);
            }
        } else {
            parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        }

        detachedMode =
                line.hasOption(DETACHED_OPTION.getOpt())
                        || line.hasOption(YARN_DETACHED_OPTION.getOpt());
        shutdownOnAttachedExit = line.hasOption(SHUTDOWN_IF_ATTACHED_OPTION.getOpt());

        this.savepointSettings = CliFrontendParser.createSavepointRestoreSettings(line);
    }

    private List<URL> loadAllJarFromPathURl(String path) throws MalformedURLException {
        // 指定需要搜索的目录.
        List<URL> urls = new ArrayList<>();
        System.out.println("jar dir:" + path);
        // 创建File对象表示目录.
        File directory = new File(path);

        // 使用FilenameFilter过滤出以.jar结尾的文件.
        File[] jarFiles =
                directory.listFiles(
                        new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return name.toLowerCase().endsWith(".jar");
                            }
                        });
        System.out.println("jarFiles len:" + jarFiles.length);
        // 遍历找到的jar文件
        if (jarFiles != null) {
            for (File jarFile : jarFiles) {
                System.out.println(jarFile.getAbsolutePath());
                URL url = jarFile.toURI().toURL();
                urls.add(url);
            }
        }
        return urls;
    }

    protected String[] extractProgramArgs(CommandLine line) {
        String[] args =
                line.hasOption(ARGS_OPTION.getOpt())
                        ? line.getOptionValues(ARGS_OPTION.getOpt())
                        : line.getArgs();

        if (args.length > 0 && !line.hasOption(JAR_OPTION.getOpt())) {
            jarFilePath = args[0];
            args = Arrays.copyOfRange(args, 1, args.length);
        }

        return args;
    }

    public void validate() throws CliArgsException {
        // Java program should be specified a JAR file
        if (getJarFilePath() == null) {
            throw new CliArgsException("Java program should be specified a JAR file.");
        }
    }

    public String getJarFilePath() {
        return jarFilePath;
    }

    public String getEntryPointClassName() {
        return entryPointClass;
    }

    public List<URL> getClasspaths() {
        return classpaths;
    }

    public String[] getProgramArgs() {
        return programArgs;
    }

    public int getParallelism() {
        return parallelism;
    }

    public boolean getDetachedMode() {
        return detachedMode;
    }

    public boolean isShutdownOnAttachedExit() {
        return shutdownOnAttachedExit;
    }

    public SavepointRestoreSettings getSavepointRestoreSettings() {
        return savepointSettings;
    }

    public void applyToConfiguration(Configuration configuration) {
        if (getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT) {
            configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, getParallelism());
        }

        configuration.setBoolean(DeploymentOptions.ATTACHED, !getDetachedMode());
        configuration.setBoolean(
                DeploymentOptions.SHUTDOWN_IF_ATTACHED, isShutdownOnAttachedExit());
        ConfigUtils.encodeCollectionToConfig(
                configuration, PipelineOptions.CLASSPATHS, getClasspaths(), URL::toString);
        SavepointRestoreSettings.toConfiguration(getSavepointRestoreSettings(), configuration);
    }

    public static ProgramOptions create(CommandLine line) throws CliArgsException {
        if (isPythonEntryPoint(line) || containsPythonDependencyOptions(line)) {
            return createPythonProgramOptions(line);
        } else {
            return new ProgramOptions(line);
        }
    }
}
