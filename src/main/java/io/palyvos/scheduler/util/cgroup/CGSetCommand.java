package io.palyvos.scheduler.util.cgroup;

import io.palyvos.scheduler.util.ExternalCommand;
import java.util.Arrays;
import java.util.List;

public class CGSetCommand implements ExternalCommand {

  private static final String EXECUTABLE = "cgset";
  private static final String PARAMETER_FLAG = "-r";

  private final String parameter;
  private final String value;
  private final String cgroupPath;

  public CGSetCommand(String cgroupPath, String parameter, String value) {
    this.parameter = parameter;
    this.value = value;
    this.cgroupPath = cgroupPath;
  }

  @Override
  public List<String> rawCommand() {
    return Arrays
        .asList(executable(), PARAMETER_FLAG, String.format("%s=%s", parameter, value), cgroupPath);
  }

  @Override
  public String executable() {
    return EXECUTABLE;
  }

  @Override
  public String toString() {
    return rawCommandAsString();
  }
}
