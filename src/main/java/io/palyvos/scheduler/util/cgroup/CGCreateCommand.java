package io.palyvos.scheduler.util.cgroup;

import io.palyvos.scheduler.util.ExternalCommand;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;

public class CGCreateCommand implements ExternalCommand {

  private static final String EXECUTABLE = "cgcreate";
  private static final String GROUPS_FLAG = "-g";

  private final String cgroupPath;
  private final String controllers;

  public CGCreateCommand(String cgroupPath, CGController... controllers) {
    Validate.notBlank(cgroupPath, "blank cgroup path");
    Validate.notEmpty(controllers, "no cgroup controller specified");
    this.cgroupPath = cgroupPath;
    this.controllers = Arrays.stream(controllers).map(c -> c.name().toLowerCase())
        .collect(Collectors.joining(","));
  }

  @Override
  public List<String> rawCommand() {
    String controllersGroups = String.format("%s:%s", controllers, cgroupPath);
    return Arrays.asList(executable(), GROUPS_FLAG, controllersGroups);
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
