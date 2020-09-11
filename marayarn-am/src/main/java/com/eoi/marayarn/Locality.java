package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Locality {
    /**
     * parse and allocate container to node and rack
     * @param candidates all nodes available
     * @param instances target instances
     * @param constraints constraints defined
     * @return for each location, return the instance count
     * @throws InvalidConstraintsSettingException
     */
    public static List<ContainerLocation> judgeLocationBy(
            List<NodeReport> candidates, int instances, int core, int memory, String constraints)
            throws InvalidConstraintsSettingException {
        if (constraints == null || constraints.isEmpty()) {
            return Collections.emptyList();
        }
        final String[] parts = constraints.split(",");
        if (parts.length < 2 || parts.length > 3) {
            throw new InvalidConstraintsSettingException("Constraints must can split to 2-3 parts by ,");
        }
        final String field = parts[0];
        final String operator = parts[1];
        String value = null;
        if (parts.length == 3) {
            value = parts[2];
        }
        if (!field.equals("node") && !field.equals("rack")) {
            throw new InvalidConstraintsSettingException("Field of constraints must be either `node` or `rack`");
        }
        List<ContainerLocation> locations = new ArrayList<>();
        if (operator.equals(Constants.CONSTRAINTS_OPERATOR_CLUSTER)) {
            if (value != null && !value.isEmpty()) {
                final String v = value;
                if (field.equals("node")) {
                    Optional<NodeReport> nodeReport = candidates.stream().filter(n -> n.getNodeId().getHost().equals(v)).findFirst();
                    NodeReport match = nodeReport.orElseThrow(() -> new InvalidConstraintsSettingException("No match node found"));
                    locations.add(new ContainerLocation(new String[]{match.getNodeId().getHost()}, null));
                } else {
                    Optional<NodeReport> nodeReport = candidates.stream().filter(n -> n.getRackName().equals(v)).findFirst();
                    NodeReport match = nodeReport.orElseThrow(() -> new InvalidConstraintsSettingException("No match rack found"));
                    locations.add(new ContainerLocation(null, new String[]{match.getRackName()}));
                }
            } else {
                if (field.equals("node")) {
                    // 如果node CLUSTER没有value, 就选一个能用的, 全部放进去
                    Optional<NodeReport> freeUp = candidates.stream().filter(
                            n -> n.getCapability().getVirtualCores() - n.getUsed().getVirtualCores() - instances * core > 0
                                    && n.getCapability().getMemory() - n.getUsed().getMemory() - instances * memory > 0)
                            .findFirst();
                    // TODO use shuffle instead of get(0)
                    String selectNode = freeUp.orElse(candidates.get(0)).getNodeId().getHost();
                    locations.add(new ContainerLocation(new String[]{selectNode}, null));
                } else {
                    // 如果rack CLUSTER没有value, 就选一个能用的, 全部放进去
                    // TODO 选择够用的rack
                    // TODO use shuffle instead of get(0)
                    String selectRack = candidates.get(0).getRackName();
                    locations.add(new ContainerLocation(null, new String[]{selectRack}));
                }
            }
        } else if (operator.equals(Constants.CONSTRAINTS_OPERATOR_LIKE)
                || operator.equals(Constants.CONSTRAINTS_OPERATOR_UNLIKE)
                || operator.equals(Constants.CONSTRAINTS_OPERATOR_IS)) {
            if (value == null || value.isEmpty()) {
                throw new InvalidConstraintsSettingException("Value part must be supplied for LIKE/UNLIKE operator");
            }
            final Pattern pattern = Pattern.compile(value);
            final String v = value;
            List<NodeReport> nodeReport;
            if (operator.equals(Constants.CONSTRAINTS_OPERATOR_LIKE)) {
                if (field.equals("node")) {
                    nodeReport = candidates.stream()
                            .filter(n -> pattern.matcher(n.getNodeId().getHost()).find())
                            .collect(Collectors.toList());
                } else {
                    nodeReport = candidates.stream()
                            .filter(n -> pattern.matcher(n.getRackName()).find())
                            .collect(Collectors.toList());
                }
            } else if (operator.equals(Constants.CONSTRAINTS_OPERATOR_UNLIKE)) {
                if (field.equals("node")) {
                    nodeReport = candidates.stream()
                            .filter(n -> !pattern.matcher(n.getNodeId().getHost()).find())
                            .collect(Collectors.toList());
                } else {
                    nodeReport = candidates.stream()
                            .filter(n -> !pattern.matcher(n.getRackName()).find())
                            .collect(Collectors.toList());
                }
            } else {
                if (field.equals("node")) {
                    nodeReport = candidates.stream()
                            .filter(n -> n.getNodeId().getHost().equals(v))
                            .collect(Collectors.toList());
                } else {
                    nodeReport = candidates.stream()
                            .filter(n -> n.getRackName().equals(v))
                            .collect(Collectors.toList());
                }
            }
            if (nodeReport.isEmpty()) {
                throw new InvalidConstraintsSettingException("No matched node or rack found");
            }
            if (field.equals("node")) {
                String[] selectNodes = nodeReport.stream()
                        .map(NodeReport::getNodeId).map(NodeId::getHost).toArray(String[]::new);
                locations.add(new ContainerLocation(selectNodes, null));
            } else {
                String[] selectRacks = nodeReport.stream()
                        .collect(Collectors.groupingBy(NodeReport::getRackName)).keySet().toArray(new String[0]);
                locations.add(new ContainerLocation(null, selectRacks));
            }
        } else if (operator.equals(Constants.CONSTRAINTS_OPERATOR_UNIQUE)
                || operator.equals(Constants.CONSTRAINTS_OPERATOR_GROUP_BY)) {
            if (field.equals("node")) {
                int each = operator.equals(Constants.CONSTRAINTS_OPERATOR_UNIQUE) ? 1 : Integer.MAX_VALUE;
                candidates.forEach(np -> locations.add(new ContainerLocation(new String[]{np.getNodeId().getHost()}, null, each)));
            } else {
                String[] uniqueRacks = candidates.stream()
                        .collect(Collectors.groupingBy(NodeReport::getRackName)).keySet().toArray(new String[0]);
                int each = operator.equals(Constants.CONSTRAINTS_OPERATOR_UNIQUE) ? 1 : Integer.MAX_VALUE;
                for (String rack: uniqueRacks) {
                    locations.add(new ContainerLocation(null, new String[]{rack}, each));
                }
            }
        } else {
            throw new InvalidConstraintsSettingException(String.format("Unsupported operator of constraints: %s", operator));
        }
        return locations;
    }
}
