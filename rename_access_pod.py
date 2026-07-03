#!/usr/bin/env python3
"""
================================================================================
ARISTA CLOUDVISION ACCESS-POD RENAME TOOL - VERSION 1.0
================================================================================

DESCRIPTION:
    Renames an Access-Pod in CloudVision by updating both the device tags and
    the studio configuration within a single workspace. This works around the
    CVP limitation that pods cannot be renamed via the UI or API.

    All changes are staged in a workspace for manual review and submission
    via Change Control.

HOW IT WORKS:
    1. Discovery: Finds all devices tagged with the old Access-Pod name and
       locates the pod in the Campus Access Interfaces studio.

    2. Validation: Checks the new name for invalid tag query characters
       (parentheses, double quotes) and verifies the new name isn't already
       in use.

    3. Preview: Shows a dry-run summary of all changes before proceeding.

    4. Tag Reassignment: Within a workspace, removes the old Access-Pod tag
       from each device and assigns the new tag value.

    5. Studio Update: Deletes the old pod entry from the studio and recreates
       it at the same position with the new tag query, preserving all
       existing interface configurations.

    6. Build: Triggers a workspace build. User must manually review, submit,
       and execute via Change Control.

USAGE:
    1. Install dependencies: pip install cloudvision grpcio cvprac --break-system-packages
    2. Update 'CV_ADDR' and 'CV_TOKEN' below.
    3. Run: python rename_access_pod.py
    4. Enter the old and new Access-Pod names when prompted.
    5. Review the preview, confirm, then open the workspace URL to submit.

VERSION HISTORY:
    v1.0 (2025-07-03): Initial release

================================================================================
"""

import sys, grpc, json, uuid, ssl
from collections import defaultdict
from google.protobuf.json_format import Parse

try:
    from google.protobuf import wrappers_pb2 as wrappers
    from arista.workspace.v1 import services as workspace_services
    from arista.workspace.v1 import workspace_pb2
    from arista.studio.v1 import services as studio_services
    from arista.studio.v1 import studio_pb2
    from arista.studio.v1.studio_pb2 import fmp_dot_wrappers__pb2
    from arista.tag.v2 import services as tag_services
    from arista.tag.v2 import tag_pb2
except ImportError as e:
    print(f"\n[!] Missing Dependency: {e.name}")
    sys.exit(1)

CV_TOKEN = "TOKEN"
CV_ADDR = "CVP_IP"

INTERFACE_STUDIO_ID = "studio-campus-access-interfaces"

# --- Shared Helpers (from access_int_vlan_check.py v1.4) ---

INVALID_TAG_QUERY_CHARS = set('()"')

def print_header(text):
    print(f"\n{'='*80}\n  {text}\n{'='*80}")

def print_step(text):
    print(f"  [i] {text}...", end=" ", flush=True)

def print_done(text="Done"):
    print(f"{text}")

def format_tag_query(label, value):
    """Build a tag query string, quoting the value if it contains spaces."""
    if ' ' in value:
        return f'{label}:"{value}"'
    return f'{label}:{value}'

def parse_tag_query_value(query, label):
    """Extract the raw value from a tag query like 'Label:value' or 'Label:"quoted value"'."""
    prefix = f'{label}:'
    if not query.startswith(prefix):
        return None
    raw = query[len(prefix):]
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    return raw

def validate_tag_value(value):
    """Return list of invalid characters found in a tag value, or empty list if valid."""
    return sorted(set(c for c in value if c in INVALID_TAG_QUERY_CHARS))

def get_grpc_channel():
    cert = ssl.get_server_certificate((CV_ADDR, 443))
    creds = grpc.ssl_channel_credentials(root_certificates=cert.encode())
    return grpc.secure_channel(f"{CV_ADDR}:443", grpc.composite_channel_credentials(
        creds, grpc.access_token_call_credentials(CV_TOKEN)))

def get_device_tags(channel):
    """Get ALL tags for all devices - returns device_id -> {tag_type: tag_value}
    Only reads COMMITTED tags (workspace_id="")"""
    stub = tag_services.TagAssignmentServiceStub(channel)
    device_tags = defaultdict(dict)

    for resp in stub.GetAll(tag_services.TagAssignmentStreamRequest()):
        workspace_id = resp.value.key.workspace_id.value
        if workspace_id != "":
            continue
        device_id = resp.value.key.device_id.value
        label = resp.value.key.label.value
        value = resp.value.key.value.value
        device_tags[device_id][label] = value

    return device_tags

def get_existing_studio_data(channel):
    """Read latest committed studio structure"""
    stub = studio_services.InputsServiceStub(channel)
    req = studio_services.InputsStreamRequest(
        partial_eq_filter=[studio_pb2.Inputs(
            key=studio_pb2.InputsKey(
                studio_id=wrappers.StringValue(value=INTERFACE_STUDIO_ID),
                workspace_id=wrappers.StringValue(value=""),
                path=fmp_dot_wrappers__pb2.RepeatedString(values=[])
            )
        )]
    )
    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            return json.loads(resp.value.inputs.value)
    return {}

def create_workspace(channel, display_name):
    """Create and return workspace ID"""
    ws_id = str(uuid.uuid4())
    stub = workspace_services.WorkspaceConfigServiceStub(channel)
    stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
        display_name=wrappers.StringValue(value=display_name)
    )))
    return ws_id

# --- Rename-specific Functions ---

def find_devices_for_pod(device_tags, pod_name):
    """Find all devices tagged with a specific Access-Pod value.
    Returns list of (device_id, hostname, full_tags_dict)."""
    results = []
    for device_id, tags in device_tags.items():
        if tags.get('Access-Pod') == pod_name:
            hostname = tags.get('hostname', device_id)
            results.append((device_id, hostname, tags))
    return results

def find_pod_in_studio(existing_data, pod_name):
    """Locate an Access-Pod in the studio data by name.
    Returns dict with location, interfaces, and full pod data, or None."""
    campus_list = existing_data.get("campus", [])

    for c, campus in enumerate(campus_list):
        campus_tag = campus.get("tags", {}).get("query", "")
        campus_name = parse_tag_query_value(campus_tag, "Campus") or f"campus{c}"

        cpods = campus.get("inputs", {}).get("campusPod", [])
        for cp, cpod in enumerate(cpods):
            cpod_tag = cpod.get("tags", {}).get("query", "")
            cpod_name = parse_tag_query_value(cpod_tag, "Campus-Pod") or f"pod{cp}"

            apods = cpod.get("inputs", {}).get("accessPod", [])
            for ap, apod in enumerate(apods):
                apod_tag = apod.get("tags", {}).get("query", "")

                if parse_tag_query_value(apod_tag, "Access-Pod") == pod_name:
                    interfaces = apod.get("inputs", {}).get("interfaces", [])
                    port_profiles = apod.get("inputs", {}).get("portProfiles", [])
                    return {
                        "location": (c, cp, ap),
                        "campus_name": campus_name,
                        "campusPod_name": cpod_name,
                        "interfaces": interfaces,
                        "port_profiles": port_profiles,
                        "full_inputs": apod.get("inputs", {})
                    }

    return None

def rename_device_tags(channel, ws_id, device_ids, old_name, new_name):
    """Reassign Access-Pod tags: remove old, add new, within workspace."""
    tag_config_stub = tag_services.TagAssignmentConfigServiceStub(channel)

    for device_id in device_ids:
        remove_payload = json.dumps({
            "value": {
                "key": {
                    "workspaceId": ws_id,
                    "elementType": 1,
                    "label": "Access-Pod",
                    "value": old_name,
                    "deviceId": device_id
                },
                "remove": True
            }
        })
        req = Parse(remove_payload, tag_services.TagAssignmentConfigSetRequest(), False)
        tag_config_stub.Set(req, timeout=30)

        add_payload = json.dumps({
            "value": {
                "key": {
                    "workspaceId": ws_id,
                    "elementType": 1,
                    "label": "Access-Pod",
                    "value": new_name,
                    "deviceId": device_id
                }
            }
        })
        req = Parse(add_payload, tag_services.TagAssignmentConfigSetRequest(), False)
        tag_config_stub.Set(req, timeout=30)

def delete_pod_from_studio(channel, ws_id, location):
    """Delete a pod entry from the studio inputs."""
    c_idx, cp_idx, ap_idx = location
    config_stub = studio_services.InputsConfigServiceStub(channel)

    path_values = [
        "campus", str(c_idx), "inputs",
        "campusPod", str(cp_idx), "inputs",
        "accessPod", str(ap_idx)
    ]

    json_request = json.dumps({
        "values": [{
            "remove": True,
            "inputs": "",
            "key": {
                "studioId": INTERFACE_STUDIO_ID,
                "workspaceId": ws_id,
                "path": {"values": path_values}
            }
        }]
    })

    req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
    for _ in config_stub.SetSome(req, timeout=30):
        pass

def create_pod_in_studio(channel, ws_id, location, new_name, pod_inputs):
    """Create a pod entry with the new name and restore all its inputs."""
    c_idx, cp_idx, ap_idx = location
    config_stub = studio_services.InputsConfigServiceStub(channel)

    pod_structure = {
        "tags": {"query": format_tag_query("Access-Pod", new_name)},
        "inputs": pod_inputs
    }

    path_values = [
        "campus", str(c_idx), "inputs",
        "campusPod", str(cp_idx), "inputs",
        "accessPod", str(ap_idx)
    ]

    json_request = json.dumps({
        "values": [{
            "remove": False,
            "inputs": json.dumps(pod_structure),
            "key": {
                "studioId": INTERFACE_STUDIO_ID,
                "workspaceId": ws_id,
                "path": {"values": path_values}
            }
        }]
    })

    req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
    for _ in config_stub.SetSome(req, timeout=30):
        pass

def build_workspace(channel, ws_id):
    """Trigger a workspace build."""
    ws_stub = workspace_services.WorkspaceConfigServiceStub(channel)
    ws_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
        request=1,
        request_params=workspace_pb2.RequestParams(request_id=wrappers.StringValue(value=str(uuid.uuid4())))
    )))

# --- Main ---

def main():
    print_header("ARISTA ACCESS-POD RENAME TOOL")

    old_name = input("\n  Enter the CURRENT Access-Pod name: ").strip()
    new_name = input("  Enter the NEW Access-Pod name: ").strip()

    if not old_name or not new_name:
        print("\n  [!] Both names are required.")
        return

    if old_name == new_name:
        print("\n  [!] Old and new names are the same.")
        return

    # --- Validate new name ---
    bad_chars = validate_tag_value(new_name)
    if bad_chars:
        print(f"\n  [!] New name contains invalid tag query characters: {' '.join(repr(c) for c in bad_chars)}")
        print(f"      CVP tag queries cannot contain: ( ) \"")
        return

    print_header("PHASE 1: DISCOVERY")

    grpc_channel = get_grpc_channel()

    print_step("Getting device tags from CloudVision")
    device_tags = get_device_tags(grpc_channel)
    print_done(f"({len(device_tags)} devices)")

    print_step(f"Finding devices tagged with Access-Pod '{old_name}'")
    affected_devices = find_devices_for_pod(device_tags, old_name)
    print_done(f"({len(affected_devices)} devices)")

    if not affected_devices:
        print(f"\n  [!] No devices found with Access-Pod tag '{old_name}'")
        return

    # Check new name isn't already in use
    existing_new = find_devices_for_pod(device_tags, new_name)
    if existing_new:
        print(f"\n  [!] Access-Pod '{new_name}' already exists ({len(existing_new)} devices)")
        print(f"      Cannot rename to an existing pod name.")
        return

    print_step("Reading interface studio data")
    existing_data = get_existing_studio_data(grpc_channel)
    print_done()

    print_step(f"Locating pod '{old_name}' in studio")
    pod_info = find_pod_in_studio(existing_data, old_name)

    if pod_info:
        c_idx, cp_idx, ap_idx = pod_info["location"]
        print_done(f"(campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx})")
    else:
        print_done("Not found in studio (tag-only pod)")

    # --- Preview ---
    print_header("PHASE 2: PREVIEW")

    print(f"\n  Rename: '{old_name}' -> '{new_name}'")
    print(f"\n  Affected devices ({len(affected_devices)}):")
    for device_id, hostname, tags in affected_devices:
        print(f"    {hostname} ({device_id})")

    if pod_info:
        c_idx, cp_idx, ap_idx = pod_info["location"]
        num_interfaces = len(pod_info["interfaces"])
        num_profiles = len(pod_info["port_profiles"])
        print(f"\n  Studio location: campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
        print(f"    Campus: {pod_info['campus_name']}")
        print(f"    Campus-Pod: {pod_info['campusPod_name']}")
        print(f"    Interfaces: {num_interfaces}")
        if num_profiles:
            print(f"    Pod-level port profiles: {num_profiles}")
    else:
        print(f"\n  Studio: No studio entry found (only device tags will be updated)")

    print(f"\n  Changes to be made:")
    print(f"    1. Remove Access-Pod:{old_name} tag from {len(affected_devices)} devices")
    print(f"    2. Add Access-Pod:{new_name} tag to {len(affected_devices)} devices")
    if pod_info:
        print(f"    3. Delete old pod entry from studio")
        print(f"    4. Recreate pod with new name (preserving {num_interfaces} interfaces)")

    confirm = input(f"\n  Continue with rename? (y/n): ").strip().lower()
    if confirm != 'y':
        print("  Aborted")
        return

    # --- Execute ---
    print_header("PHASE 3: CREATE WORKSPACE")

    display = f"Rename_{old_name[:20]}_{new_name[:20]}".replace(" ", "_")
    print_step("Creating workspace")
    ws_id = create_workspace(grpc_channel, display)
    print_done(f"({ws_id[:8]})")

    print_header("PHASE 4: REASSIGN DEVICE TAGS")

    device_ids = [d[0] for d in affected_devices]
    print_step(f"Updating tags on {len(device_ids)} devices")
    rename_device_tags(grpc_channel, ws_id, device_ids, old_name, new_name)
    print_done()

    if pod_info:
        print_header("PHASE 5: UPDATE STUDIO")

        location = pod_info["location"]

        print_step("Deleting old pod entry")
        delete_pod_from_studio(grpc_channel, ws_id, location)
        print_done()

        print_step("Creating pod with new name")
        create_pod_in_studio(grpc_channel, ws_id, location, new_name, pod_info["full_inputs"])
        print_done()

        print(f"\n    Preserved {len(pod_info['interfaces'])} interfaces")

    print_header("PHASE 6: BUILD WORKSPACE")

    print_step("Triggering build")
    build_workspace(grpc_channel, ws_id)
    print_done()

    print_header("SUCCESS")
    print(f"  Workspace: https://{CV_ADDR}/cv/provisioning/workspaces?ws={ws_id}")
    print(f"  Renamed: '{old_name}' -> '{new_name}'")
    print(f"  Devices updated: {len(affected_devices)}")
    if pod_info:
        print(f"  Interfaces preserved: {len(pod_info['interfaces'])}")
    print(f"\n  Open the workspace URL to review and submit changes.")

if __name__ == "__main__":
    main()
