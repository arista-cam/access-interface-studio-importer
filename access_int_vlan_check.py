#!/usr/bin/env python3
"""
================================================================================
ARISTA CLOUDVISION BULK IMPORTER - VERSION 1.5
================================================================================

DESCRIPTION:
    This production-ready script automates the provisioning of Campus Access 
    Interfaces in CloudVision Studios.

    It reads a CSV of port mappings, validates that all required VLANs exist
    in the topology studio, intelligently discovers Access-Pods (even those
    with 0 configured interfaces), merges new configuration with existing
    Studio data, creates a new workspace, and builds the changes.
    Changes need to be reviewed and accepted then pushed via a Change Control.

ENHANCEMENT IN v1.5:
    - VLAN Creation: When missing VLANs are detected, offers to create them
      from a customer-provided VLAN CSV (VLAN ID + Name columns)
    - Writes VLANs to the AVD Campus Topology studio for matching Campus-Pods
    - CSV Header Validation: Validates CSV columns on selection to prevent
      accidentally choosing the wrong file
    - Reusable CSV Picker: Extracted file selection into shared function

ENHANCEMENT IN v1.4:
    - Tag Query Safety: Properly quotes tag values containing spaces
    - Pre-flight Validation: Detects invalid characters (parentheses, quotes)
      in Access-Pod tag names before attempting import
    - Robust Tag Parsing: Handles quoted and unquoted tag query formats when
      matching pods in studio data

ENHANCEMENT IN v1.3:
    - Smart Pod Discovery: Finds Access-Pods WITH or WITHOUT existing interfaces
    - Tag-Based Fallback: When pod not found in studio, queries device tags
    - Auto Pod Creation: Creates pod structure for empty Access-Pods
    - Hierarchy Detection: Determines campus/campusPod location from device tags

    This solves the "Pod not found" error for Access-Pods that exist but have
    0 configured interfaces (which don't appear in studio committed state).

HOW IT WORKS:
    1. Discovery: Maps device hostnames to UUIDs via CloudVision device tags.
       Groups interfaces by their Access-Pod tag value.
    
    2. VLAN Validation: Queries the AVD Campus Topology studio to ensure all
       required VLANs (Access + Voice) are defined. If missing, offers to
       create them from a VLAN CSV (writes to topology studio, then exits
       so user can review/submit before re-running for interface import).
    
    3. Pod Location (TWO-STAGE):
       - PRIMARY: Queries studio inputs for pods with interfaces
       - FALLBACK: If not found, queries device tags to verify pod exists
       - Auto-creates pod structure if devices exist but no studio config
    
    4. Port Profiles: Auto-generates smart profiles (Trunk/Phone/Access) and
       creates any missing profiles in the workspace.
    
    5. Interface Writing: Writes interfaces sequentially starting from the
       existing interface count (or 0 for new pods). Uses SetSome to write
       each interface individually.
    
    6. Workspace Build: Creates workspace, writes all data, triggers build.
       User must manually review, submit, and execute via Change Control.

USAGE:
    1. Ensure the 'CSV' folder exists and contains your interface mapping files.
    2. Install dependencies: `pip install cloudvision grpcio pandas cvprac --break-system-packages`
    3. Update 'CV_ADDR' and 'CV_TOKEN' in the Configuration section below.
    4. Run: python arista_importer_enhanced.py
    5. Select your CSV file from the menu.
    6. Review output, confirm import, open workspace URL to submit changes.

CSV STRUCTURE REQUIREMENTS:
    The CSV must contain the following headers:
    - New_Switch:   The hostname of the switch (as seen in CloudVision).
    - Port:         The port number (e.g., 1, 5, 12).
    - Mode:         'access' or 'trunk'.
    - Port Profile: The name of the profile (e.g., A18-V510, TRUNK_DEFAULT).
    - Access:       The data/native VLAN ID.
    - Voice:        (Optional) The voice VLAN ID for phones.
    - Description:  (Optional) Interface description.

MAPPING LOGIC:
    - If Mode is 'trunk': Uses generic 'TRUNK_DEFAULT' profile (Allow All).
    - If Mode is 'access' and Voice VLAN exists: Uses 'trunk phone' mode.
    - If Mode is 'access' and NO Voice VLAN: Uses 'access' mode.

VERSION HISTORY:
    v1.5 (2025-08-18): VLAN creation from CSV, CSV header validation
    v1.4 (2025-07-03): Tag query quoting, pre-flight validation for invalid chars
    v1.3 (2025-02-04): Enhanced with tag-based fallback for empty pods
    v1.2: Added VLAN validation and port profile auto-generation
    v1.1: Safe merge with existing data, multiple pod support
    v1.0: Initial release

================================================================================
"""

import sys, csv, grpc, json, uuid, ssl, time, os, re
from collections import defaultdict
from google.protobuf.json_format import Parse

try:
    import pandas as pd
    from cvprac.cvp_client import CvpClient
    from google.protobuf import wrappers_pb2 as wrappers
    from arista.workspace.v1 import services as workspace_services
    from arista.workspace.v1 import workspace_pb2
    from arista.studio.v1 import services as studio_services
    from arista.studio.v1 import studio_pb2
    from arista.studio.v1.studio_pb2 import fmp_dot_wrappers__pb2
    from arista.inventory.v1 import services as inventory_services
    from arista.tag.v2 import services as tag_services
    from arista.tag.v2 import tag_pb2
except ImportError as e:
    print(f"\n[!] Missing Dependency: {e.name}")
    sys.exit(1)

CV_TOKEN = "API TOKEN"
CV_ADDR = "CVP IP ADDRESS"

INTERFACE_STUDIO_ID = "studio-campus-access-interfaces"
TOPOLOGY_STUDIO_ID = "studio-avd-campus-fabric"

def print_header(text):
    print(f"\n{'='*80}\n  {text}\n{'='*80}")

def print_step(text):
    print(f"  [i] {text}...", end=" ", flush=True)

def print_done(text="Done"):
    print(f"{text}")

def get_grpc_channel():
    cert = ssl.get_server_certificate((CV_ADDR, 443))
    creds = grpc.ssl_channel_credentials(root_certificates=cert.encode())
    return grpc.secure_channel(f"{CV_ADDR}:443", grpc.composite_channel_credentials(
        creds, grpc.access_token_call_credentials(CV_TOKEN)))

def get_inventory_map(channel):
    """Returns hostname -> device_id mapping"""
    stub = inventory_services.DeviceServiceStub(channel)
    mapping = {}
    for resp in stub.GetAll(inventory_services.DeviceStreamRequest()):
        if resp.value.hostname.value:
            mapping[resp.value.hostname.value] = resp.value.key.device_id.value
    return mapping

def clean_int_str(s):
    """Remove .0 from strings like '1674.0'"""
    return s.replace('.0', '') if s else s

INVALID_TAG_QUERY_CHARS = set('()"')

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

def build_profile_object(row):
    """Build port-profile object from CSV row. Returns None for trunk ports."""
    mode = str(row.get('Mode', '')).strip().lower()
    if mode == "trunk":
        return None
    
    pname = str(row.get('Port Profile', '')).strip()
    if not pname or pname.lower() == "nan": 
        return None
    
    v_raw = clean_int_str(str(row.get('Voice', '')).strip())
    a_raw = clean_int_str(str(row.get('Access', '')).strip())
    vv = int(v_raw) if v_raw and v_raw.isdigit() else None
    av = int(a_raw) if a_raw and a_raw.isdigit() else None
    
    if not av:
        m = re.search(r'A(\d+)', pname)
        if m: 
            av = int(m.group(1))
    if not vv:
        m = re.search(r'V(\d+)', pname)
        if m: 
            vv = int(m.group(1))
    
    p_mode = "trunk phone" if (vv or "V" in pname) else "access"
    p_obj = {
        "parentProfile": "BASE",
        "name": pname, 
        "mode": p_mode, 
        "enabled": "Yes", 
        "vlans": {}, 
        "spanningTree": {"portfast": "edge"}
    }
    
    if p_mode == "access" and av:
        p_obj["vlans"]["vlans"] = str(av)
    elif p_mode == "trunk phone":
        if av: 
            p_obj["vlans"]["nativeVlan"] = av
        if vv: 
            p_obj["vlans"]["phoneVlan"] = vv
    
    return p_obj

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

def find_devices_by_hostname(device_tags, hostname):
    """Find device ID(s) that match a hostname"""
    matches = []
    for device_id, tags in device_tags.items():
        if tags.get('hostname') == hostname:
            matches.append((device_id, tags))
    return matches

def verify_pod_exists_in_tags(channel, pod_name):
    """
    NEW FUNCTION: Check if an Access-Pod exists via device tags.
    Uses GetAll() to query all tag assignments, then filters for the pod.
    Returns (exists: bool, device_count: int, sample_device_id: str or None)
    """
    tag_stub = tag_services.TagAssignmentServiceStub(channel)
    
    device_ids = []
    
    try:
        for resp in tag_stub.GetAll(tag_services.TagAssignmentStreamRequest()):
            label = resp.value.key.label.value
            value = resp.value.key.value.value
            device_id = resp.value.key.device_id.value
            
            if label == "Access-Pod" and value == pod_name and device_id:
                if device_id not in device_ids:
                    device_ids.append(device_id)
    
    except grpc.RpcError as e:
        print(f"\n      Warning: Tag query failed: {e}")
        return False, 0, None
    
    exists = len(device_ids) > 0
    sample = device_ids[0] if device_ids else None
    
    return exists, len(device_ids), sample

def find_pod_hierarchy_from_device(channel, device_id, existing_data):
    """
    NEW FUNCTION: Find where a pod should be created by querying device's Campus/Campus-Pod tags.
    Uses GetAll() to query all tags for this device.
    Returns (campus_idx, campusPod_idx, next_accessPod_idx) or None
    """
    tag_stub = tag_services.TagAssignmentServiceStub(channel)
    
    campus_name = None
    campusPod_name = None
    
    try:
        for resp in tag_stub.GetAll(tag_services.TagAssignmentStreamRequest()):
            resp_device_id = resp.value.key.device_id.value
            
            if resp_device_id == device_id:
                label = resp.value.key.label.value
                value = resp.value.key.value.value
                
                if label == "Campus":
                    campus_name = value
                elif label == "Campus-Pod":
                    campusPod_name = value
                
                if campus_name and campusPod_name:
                    break
    
    except grpc.RpcError as e:
        print(f"\n      Warning: Failed to query device tags: {e}")
        return None
    
    if not campus_name or not campusPod_name:
        return None
    
    campus_list = existing_data.get("campus", [])
    
    for c_idx, campus in enumerate(campus_list):
        campus_tag = campus.get("tags", {}).get("query", "")
        if parse_tag_query_value(campus_tag, "Campus") != campus_name:
            continue

        cpods = campus.get("inputs", {}).get("campusPod", [])
        for cp_idx, cpod in enumerate(cpods):
            cpod_tag = cpod.get("tags", {}).get("query", "")
            if parse_tag_query_value(cpod_tag, "Campus-Pod") != campusPod_name:
                continue
            
            apods = cpod.get("inputs", {}).get("accessPod", [])
            max_ap_idx = len(apods) - 1 if apods else -1
            next_ap_idx = max_ap_idx + 1
            
            return c_idx, cp_idx, next_ap_idx
    
    return None

def get_existing_studio_data(channel):
    """Read latest studio structure to find pod indices"""
    
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

def get_existing_port_profiles(existing_data):
    """Extract all existing port-profile names from studio data"""
    existing_profiles = set()
    
    top_level_profiles = existing_data.get("portProfiles", [])
    for pp in top_level_profiles:
        if pp and pp.get("name"):
            existing_profiles.add(pp["name"])
    
    campus_list = existing_data.get("campus", [])
    
    for campus in campus_list:
        for pp in campus.get("inputs", {}).get("portProfiles", []):
            if pp and pp.get("name"):
                existing_profiles.add(pp["name"])
        
        for cpod in campus.get("inputs", {}).get("campusPod", []):
            for pp in cpod.get("inputs", {}).get("portProfiles", []):
                if pp and pp.get("name"):
                    existing_profiles.add(pp["name"])
            
            for apod in cpod.get("inputs", {}).get("accessPod", []):
                for pp in apod.get("inputs", {}).get("portProfiles", []):
                    if pp and pp.get("name"):
                        existing_profiles.add(pp["name"])
    
    return existing_profiles

def create_workspace(channel):
    """Create and return workspace ID"""
    ws_id = str(uuid.uuid4())
    stub = workspace_services.WorkspaceConfigServiceStub(channel)
    stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
        display_name=wrappers.StringValue(value=f"Import_{ws_id[:8]}")
    )))
    return ws_id

def get_vlans_from_topology_studio(channel):
    """Get all VLANs defined in the AVD Campus Topology studio"""
    stub = studio_services.InputsServiceStub(channel)
    
    req = studio_services.InputsStreamRequest(
        partial_eq_filter=[studio_pb2.Inputs(
            key=studio_pb2.InputsKey(
                studio_id=wrappers.StringValue(value=TOPOLOGY_STUDIO_ID),
                workspace_id=wrappers.StringValue(value=""),
                path=fmp_dot_wrappers__pb2.RepeatedString(values=[])
            )
        )]
    )
    
    vlans = set()
    
    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            topology_data = json.loads(resp.value.inputs.value)
            
            campus_services = topology_data.get("campusServices", [])
            
            for service_entry in campus_services:
                service_group = service_entry.get("inputs", {}).get("campusServicesGroup", {})
                campus_pods_services = service_group.get("campusPodsServices", [])
                
                for cpod_service in campus_pods_services:
                    services = cpod_service.get("inputs", {}).get("services", {})
                    svis = services.get("svis", [])
                    
                    for svi in svis:
                        vlan_id = svi.get("id")
                        if vlan_id:
                            vlans.add(int(vlan_id))
    
    return vlans

def get_topology_pod_structure(channel):
    """Read the topology studio structure and return campus pod info for VLAN creation.
    Returns dict: campus_pod_name -> {campus_idx, cpod_idx, svi_count, existing_vlan_ids}"""
    stub = studio_services.InputsServiceStub(channel)

    req = studio_services.InputsStreamRequest(
        partial_eq_filter=[studio_pb2.Inputs(
            key=studio_pb2.InputsKey(
                studio_id=wrappers.StringValue(value=TOPOLOGY_STUDIO_ID),
                workspace_id=wrappers.StringValue(value=""),
                path=fmp_dot_wrappers__pb2.RepeatedString(values=[])
            )
        )]
    )

    pod_structure = {}

    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            topology_data = json.loads(resp.value.inputs.value)

            campus_services = topology_data.get("campusServices", [])

            for c_idx, service_entry in enumerate(campus_services):
                campus_query = service_entry.get("tags", {}).get("query", "")
                campus_name = parse_tag_query_value(campus_query, "Campus")

                service_group = service_entry.get("inputs", {}).get("campusServicesGroup", {})
                campus_pods_services = service_group.get("campusPodsServices", [])

                for cp_idx, cpod_service in enumerate(campus_pods_services):
                    cpod_query = cpod_service.get("tags", {}).get("query", "")
                    cpod_name = parse_tag_query_value(cpod_query, "Campus-Pod")

                    if not cpod_name:
                        continue

                    services = cpod_service.get("inputs", {}).get("services", {})
                    svis = services.get("svis", [])

                    existing_ids = set()
                    for svi in svis:
                        vlan_id = svi.get("id")
                        if vlan_id:
                            existing_ids.add(int(vlan_id))

                    pod_structure[cpod_name] = {
                        "campus_idx": c_idx,
                        "campus_name": campus_name,
                        "cpod_idx": cp_idx,
                        "svi_count": len(svis),
                        "existing_vlan_ids": existing_ids
                    }

    return pod_structure

def _write_svi_field(config_stub, ws_id, c_idx, cp_idx, svi_idx, field_name, value):
    """Write a single SVI field to the topology studio."""
    path_values = [
        "campusServices", str(c_idx), "inputs",
        "campusServicesGroup", "campusPodsServices", str(cp_idx),
        "inputs", "services", "svis", str(svi_idx), field_name
    ]

    json_request = json.dumps({
        "values": [{
            "remove": False,
            "inputs": json.dumps(value),
            "key": {
                "studioId": TOPOLOGY_STUDIO_ID,
                "workspaceId": ws_id,
                "path": {"values": path_values}
            }
        }]
    })

    req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
    for response in config_stub.SetSome(req, timeout=30):
        pass

def create_vlans_in_topology(channel, ws_id, vlans_to_create, topology_pod_info,
                              target_cpods, vlan_to_access_pods, device_campus_pod_map):
    """Write missing VLANs to the topology studio for the specified campus pods.
    vlans_to_create: {vlan_id: name}
    target_cpods: set of Campus-Pod names to add VLANs to
    vlan_to_access_pods: {vlan_id: set of Access-Pod names}
    device_campus_pod_map: {access_pod_name: campus_pod_name}"""
    config_stub = studio_services.InputsConfigServiceStub(channel)

    total_written = 0

    for cpod_name in target_cpods:
        pod_info = topology_pod_info.get(cpod_name)
        if not pod_info:
            print(f"    [!] Campus-Pod '{cpod_name}' not found in topology studio - skipping")
            continue

        c_idx = pod_info["campus_idx"]
        cp_idx = pod_info["cpod_idx"]
        existing_ids = pod_info["existing_vlan_ids"]
        svi_offset = pod_info["svi_count"]

        vlans_for_pod = {vid: name for vid, name in vlans_to_create.items() if vid not in existing_ids}

        if not vlans_for_pod:
            print(f"    {cpod_name}: all VLANs already exist")
            continue

        print(f"    {cpod_name}: writing {len(vlans_for_pod)} VLANs...")

        for i, (vlan_id, vlan_name) in enumerate(sorted(vlans_for_pod.items())):
            svi_idx = svi_offset + i

            _write_svi_field(config_stub, ws_id, c_idx, cp_idx, svi_idx, "id", vlan_id)
            _write_svi_field(config_stub, ws_id, c_idx, cp_idx, svi_idx, "name", vlan_name)
            _write_svi_field(config_stub, ws_id, c_idx, cp_idx, svi_idx, "enabled", "Yes")

            access_pods_for_vlan = [
                ap for ap, cp in device_campus_pod_map.items()
                if cp == cpod_name and vlan_id in vlan_to_access_pods.get(ap, set())
            ]

            if access_pods_for_vlan:
                devices_array = []
                for ap_name in sorted(access_pods_for_vlan):
                    devices_array.append({
                        "tagQuery": {"tags": {"query": format_tag_query("Access-Pod", ap_name)}},
                        "ipVirtualRouterSubnet": None,
                        "enabled": None
                    })
                _write_svi_field(config_stub, ws_id, c_idx, cp_idx, svi_idx, "devices", devices_array)

            total_written += 1
            print(f"      VLAN {vlan_id} ({vlan_name})")

        print(f"    {cpod_name}: done")

    return total_written

def locate_pod_in_studio(grpc_channel, pod_name, existing_data, device_tags, device_campus_pod_map):
    """
    Locate a single pod in the studio and return its details.
    Returns: dict with location, existing_count, needs_creation, or None if error
    """
    found_pods = []
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
                interface_count = len(apod.get("inputs", {}).get("interfaces", []))

                if parse_tag_query_value(apod_tag, "Access-Pod") == pod_name:
                    found_pods.append({
                        "location": (c, cp, ap),
                        "pod_name": pod_name,
                        "total_interfaces": interface_count,
                        "tag": apod_tag,
                        "campus_name": campus_name,
                        "campusPod_name": cpod_name
                    })
    
    if not found_pods:
        exists, device_count, sample_device = verify_pod_exists_in_tags(grpc_channel, pod_name)
        
        if not exists:
            print(f"\n[!] ERROR: Pod '{pod_name}' does not exist in device tags!")
            return None
        
        hierarchy = find_pod_hierarchy_from_device(grpc_channel, sample_device, existing_data)
        
        if not hierarchy:
            print(f"\n[!] ERROR: Could not determine location for pod '{pod_name}'")
            return None
        
        c_idx, cp_idx, ap_idx = hierarchy
        
        return {
            "location": (c_idx, cp_idx, ap_idx),
            "pod_name": pod_name,
            "total_interfaces": 0,
            "needs_creation": True
        }
    
    if len(found_pods) > 1:
        expected_campusPod = device_campus_pod_map.get(pod_name, None)
        
        if expected_campusPod:
            matching_pods = [p for p in found_pods if p.get("campusPod_name") == expected_campusPod]
            
            if len(matching_pods) == 1:
                found_pods = matching_pods
            elif len(matching_pods) > 1:
                found_pods = matching_pods
        
        if len(found_pods) > 1:
            found_pods = [found_pods[0]]
    
    return found_pods[0]

def validate_vlans_in_topology(csv_data, topology_vlans):
    """Validate that required VLANs exist in the AVD Campus Topology.
    Returns (passed: bool, missing_vlans: set)."""
    print_step("Validating VLANs against topology design")

    required_vlans = set()

    for row in csv_data:
        mode_col = str(row.get('Mode', '')).strip().lower()
        if mode_col == "trunk":
            continue

        for col in ['Access', 'Voice']:
            val = clean_int_str(str(row.get(col, '')).strip())
            if val and val.isdigit():
                vlan = int(val)
                if vlan != 1:
                    required_vlans.add(vlan)

        profile = str(row.get('Port Profile', ''))
        for m in re.findall(r'[AV](\d+)', profile):
            vlan = int(m)
            if vlan != 1:
                required_vlans.add(vlan)

    missing = required_vlans - topology_vlans

    if missing:
        print_done("FAILED")
        print("\n" + "!"*80)
        print("  CRITICAL VALIDATION FAILURE: MISSING VLANS IN TOPOLOGY")
        print("!"*80)
        print(f"\n  The following VLANs are not defined in the AVD Campus Topology studio:")
        print(f"  {', '.join([str(v) for v in sorted(missing)])}")
        print("!"*80)
        return False, missing

    print_done(f"Passed ({len(required_vlans)} VLANs verified)")
    return True, set()

def select_csv_file(prompt_label="Select CSV file", required_columns=None):
    """Present CSV file picker and validate headers. Returns filepath or None."""
    csv_dir = "./CSV"

    while True:
        csv_path = None

        if os.path.exists(csv_dir):
            csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
            if csv_files:
                print(f"\nAvailable CSV files in {csv_dir}:")
                for i, f in enumerate(csv_files, 1):
                    print(f"  [{i}] {f}")

                choice = input(f"\n{prompt_label} [1-{len(csv_files)}]: ").strip()
                try:
                    csv_idx = int(choice) - 1
                    if 0 <= csv_idx < len(csv_files):
                        csv_path = os.path.join(csv_dir, csv_files[csv_idx])
                    else:
                        print("Invalid choice")
                        return None
                except:
                    print("Invalid input")
                    return None
            else:
                print(f"\nNo CSV files found in {csv_dir}")
                csv_path = input("Enter CSV file path: ").strip()
        else:
            print(f"\nCSV directory not found: {csv_dir}")
            csv_path = input("Enter CSV file path: ").strip()

        if not os.path.exists(csv_path):
            print(f"File not found: {csv_path}")
            return None

        if required_columns:
            with open(csv_path) as f:
                reader = csv.DictReader(f)
                headers = set(reader.fieldnames or [])
            missing = [c for c in required_columns if c not in headers]
            if missing:
                print(f"\n  [!] CSV is missing required columns: {', '.join(missing)}")
                print(f"      Found columns: {', '.join(sorted(headers))}")
                print(f"      Is this the right file?")
                retry = input("\n  Select a different file? (y/n): ").strip().lower()
                if retry == 'y':
                    continue
                return None

        print(f"\nUsing CSV: {csv_path}")
        return csv_path

def read_vlan_csv(filepath):
    """Read VLAN CSV and return {vlan_id (int): name (str)}."""
    with open(filepath) as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        id_col = None
        name_col = None
        for h in headers:
            hl = h.lower()
            if id_col is None and ('vlan' in hl or 'id' in hl):
                id_col = h
            if name_col is None and 'name' in hl:
                name_col = h

        if not id_col or not name_col:
            print(f"  [!] Could not detect VLAN ID and Name columns")
            print(f"      Found columns: {', '.join(headers)}")
            return None

        print(f"  Using columns: ID='{id_col}', Name='{name_col}'")

        vlan_map = {}
        for row in reader:
            # 1. Extract string, convert to lowercase
            raw_id_str = str(row.get(id_col, '')).lower()
            
            # 2. Remove the word 'vlan' and strip any leftover spaces (e.g. "vlan 10" -> "10")
            raw_id_str = raw_id_str.replace('vlan', '').strip()
            
            # 3. Pass the cleaned string into your existing function
            raw_id = clean_int_str(raw_id_str)
            
            name = str(row.get(name_col, '')).strip()
            
            if raw_id and raw_id.isdigit() and name:
                vlan_map[int(raw_id)] = name

        return vlan_map

def main():
    print_header("ARISTA IMPORTER")

    INTERFACE_CSV_COLUMNS = ['New_Switch', 'Port', 'Mode', 'Port Profile', 'Access']
    CSV_FILE = select_csv_file("Select interface CSV file", required_columns=INTERFACE_CSV_COLUMNS)
    if not CSV_FILE:
        return
    print()
    
    grpc_channel = get_grpc_channel()
    
    print_header("PHASE 1: DISCOVERY")
    print_step("Reading CSV")
    with open(CSV_FILE) as f:
        reader = csv.DictReader(f)
        csv_data = [row for row in reader]
    print_done(f"({len(csv_data)} rows)")
    
    print_step("Getting device tags from CloudVision")
    device_tags = get_device_tags(grpc_channel)
    print_done(f"({len(device_tags)} devices)")
    
    print("\n    Mapping CSV hostnames to devices:")
    hostname_to_device = {}
    for row in csv_data[:5]:
        hostname = str(row['New_Switch']).strip()
        if hostname not in hostname_to_device:
            matches = find_devices_by_hostname(device_tags, hostname)
            if matches:
                device_id, tags = matches[0]
                pod_name = tags.get('Access-Pod', 'Unknown')
                hostname_to_device[hostname] = (device_id, pod_name)
                print(f"      {hostname} → {device_id} (Pod: {pod_name})")
            else:
                print(f"      {hostname} → NOT FOUND")
    
    if not hostname_to_device:
        print("\n[!] ERROR: No CSV hostnames found in CloudVision tags")
        return
    
    print_header("PHASE 2: PROCESSING CSV")
    interfaces_by_pod = defaultdict(list)
    trunk_ports = []
    device_campus_pod_map = {}
    
    for row in csv_data:
        switch = str(row['New_Switch']).strip()
        port = str(row['Port']).strip()
        mode = str(row.get('Mode', '')).strip().lower()
        profile = str(row.get('Port Profile', 'TRUNK_DEFAULT')).strip()
        desc = str(row.get('Description', '')).strip()
        
        if mode == "trunk":
            trunk_ports.append(f"{switch} Ethernet{port}")
            continue
        
        matches = find_devices_by_hostname(device_tags, switch)
        if not matches:
            continue
        
        device_id, tags = matches[0]
        pod_name = tags.get('Access-Pod', 'Unknown')
        
        if pod_name not in device_campus_pod_map:
            device_campus_pod_map[pod_name] = tags.get('Campus-Pod', 'Unknown')
        
        interface_data = {
            "tags": {"query": f"interface:Ethernet{port}@{device_id}"},
            "inputs": {
                "adapterDetails": {
                    "portProfile": profile,
                    "enabled": "Yes",
                    "description": desc if desc else None,
                    "portChannel": {},
                    "vlans": {"vlans": None}
                }
            }
        }
        
        interfaces_by_pod[pod_name].append({
            "device_id": device_id,
            "interface_data": interface_data
        })
    
    print_done(f"Grouped {sum(len(v) for v in interfaces_by_pod.values())} interfaces into {len(interfaces_by_pod)} pods")
    
    for pod_name, interfaces in interfaces_by_pod.items():
        print(f"    {pod_name}: {len(interfaces)} interfaces")

    invalid_pods = []
    for pod_name in interfaces_by_pod.keys():
        bad_chars = validate_tag_value(pod_name)
        if bad_chars:
            invalid_pods.append((pod_name, bad_chars))

    if invalid_pods:
        print("\n" + "!"*80)
        print("  CRITICAL: INVALID CHARACTERS IN ACCESS-POD TAG NAMES")
        print("!"*80)
        print("\n  CVP tag queries cannot contain: ( ) \"")
        print("  The following pods have invalid characters that must be fixed in CloudVision:\n")
        for pod_name, chars in invalid_pods:
            print(f"    Pod: {pod_name}")
            print(f"    Invalid chars: {' '.join(repr(c) for c in chars)}")
        print(f"\n  Please rename these Access-Pod tags in CloudVision to remove")
        print(f"  parentheses and double quotes, then re-run the script.")
        print("!"*80)
        return

    print_header("PHASE 2.5: VLAN VALIDATION")

    print_step("Reading VLANs from topology studio")
    topology_vlans = get_vlans_from_topology_studio(grpc_channel)
    print_done(f"({len(topology_vlans)} VLANs defined)")

    vlans_ok, missing_vlans = validate_vlans_in_topology(csv_data, topology_vlans)
    if not vlans_ok:
        print(f"\n  Would you like to create these {len(missing_vlans)} VLANs from a CSV?")
        answer = input("  (y/n): ").strip().lower()
        if answer != 'y':
            print("\n    Aborting due to missing VLANs")
            return

        vlan_csv = select_csv_file("Select VLAN CSV file")
        if not vlan_csv:
            return

        vlan_map = read_vlan_csv(vlan_csv)
        if vlan_map is None:
            return

        creatable = {v: vlan_map[v] for v in missing_vlans if v in vlan_map}
        uncreatable = missing_vlans - set(vlan_map.keys())

        if creatable:
            print(f"\n  VLANs to create ({len(creatable)}):")
            for vid in sorted(creatable):
                print(f"    VLAN {vid:>4} → {creatable[vid]}")

        if uncreatable:
            print(f"\n" + "!"*80)
            print(f"  WARNING: {len(uncreatable)} missing VLANs not found in VLAN CSV:")
            print(f"  {', '.join([str(v) for v in sorted(uncreatable)])}")
            print(f"  These VLANs must be added manually before interface import.")
            print("!"*80)
            print("\n    Aborting - all missing VLANs must be resolvable")
            return

        access_pod_vlans = defaultdict(set)
        for row in csv_data:
            mode_col = str(row.get('Mode', '')).strip().lower()
            if mode_col == "trunk":
                continue
            switch = str(row['New_Switch']).strip()
            matches = find_devices_by_hostname(device_tags, switch)
            if not matches:
                continue
            _, tags = matches[0]
            ap_name = tags.get('Access-Pod', 'Unknown')
            for col in ['Access', 'Voice']:
                val = clean_int_str(str(row.get(col, '')).strip())
                if val and val.isdigit():
                    vlan = int(val)
                    if vlan != 1:
                        access_pod_vlans[ap_name].add(vlan)
            profile = str(row.get('Port Profile', ''))
            for m in re.findall(r'[AV](\d+)', profile):
                vlan = int(m)
                if vlan != 1:
                    access_pod_vlans[ap_name].add(vlan)

        target_cpods = set(device_campus_pod_map.values())

        print_step("Reading topology studio structure")
        topology_pod_info = get_topology_pod_structure(grpc_channel)
        print_done(f"({len(topology_pod_info)} campus pods found)")

        matched_cpods = target_cpods & set(topology_pod_info.keys())
        unmatched_cpods = target_cpods - set(topology_pod_info.keys())

        if unmatched_cpods:
            print(f"\n  [!] WARNING: These Campus-Pods were not found in topology studio:")
            for cp in sorted(unmatched_cpods):
                print(f"      {cp}")

        if not matched_cpods:
            print("\n  [!] ERROR: No matching Campus-Pods found in topology studio")
            return

        print(f"\n  Target Campus-Pods: {', '.join(sorted(matched_cpods))}")
        confirm = input(f"\n  Proceed with VLAN creation? (y/n): ").strip().lower()
        if confirm != 'y':
            return

        print_header("VLAN CREATION")

        print_step("Creating workspace")
        ws_id = create_workspace(grpc_channel)
        print_done(f"({ws_id[:8]})")

        total = create_vlans_in_topology(grpc_channel, ws_id, creatable, topology_pod_info,
                                          matched_cpods, access_pod_vlans, device_campus_pod_map)

        print_step("Triggering build")
        ws_stub = workspace_services.WorkspaceConfigServiceStub(grpc_channel)
        ws_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
            key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
            request=1,
            request_params=workspace_pb2.RequestParams(request_id=wrappers.StringValue(value=str(uuid.uuid4())))
        )))
        print_done()

        print_header("VLAN CREATION COMPLETE")
        print(f"  Workspace: https://{CV_ADDR}/cv/provisioning/workspaces?ws={ws_id}")
        print(f"  VLANs written: {total}")
        print(f"\n  Next steps:")
        print(f"    1. Open the workspace URL above")
        print(f"    2. Review the VLAN changes in the topology studio")
        print(f"    3. Submit the workspace and execute the Change Control")
        print(f"    4. Re-run this script to continue with the interface import")
        return
    
    print_header("PHASE 3: LOCATE PODS IN STUDIO")
    
    print_step("Reading interface studio")
    existing_data = get_existing_studio_data(grpc_channel)
    print_done()
    
    print(f"\n  Locating {len(interfaces_by_pod)} Access-Pods in studio...")
    
    pod_locations = {}
    
    for pod_name in interfaces_by_pod.keys():
        print(f"\n  [{pod_name}]")
        pod_info = locate_pod_in_studio(grpc_channel, pod_name, existing_data, device_tags, device_campus_pod_map)
        
        if not pod_info:
            print(f"    ✗ Failed to locate pod '{pod_name}' - aborting")
            return
        
        c_idx, cp_idx, ap_idx = pod_info["location"]
        existing_count = pod_info.get("total_interfaces", 0)
        needs_creation = pod_info.get("needs_creation", False)
        
        print(f"    Location: campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
        print(f"    Existing interfaces: {existing_count}")
        print(f"    Will add: {len(interfaces_by_pod[pod_name])} interfaces")
        
        if needs_creation:
            print(f"    Status: Will create new pod structure")
        
        pod_locations[pod_name] = pod_info
    
    total_interfaces_to_add = sum(len(v) for v in interfaces_by_pod.values())
    total_existing = sum(pod_locations[p].get("total_interfaces", 0) for p in interfaces_by_pod.keys())
    
    print(f"\n  Summary:")
    print(f"    Total pods: {len(interfaces_by_pod)}")
    print(f"    Total existing interfaces: {total_existing}")
    print(f"    Total new interfaces: {total_interfaces_to_add}")
    print(f"    Total after import: {total_existing + total_interfaces_to_add}")
    
    confirm = input(f"\n  Continue with import? (y/n): ").strip().lower()
    if confirm != 'y':
        print("    Aborted")
        return
    
    print_header("PHASE 3.5: WORKSPACE")
    
    print_step("Creating workspace")
    ws_id = create_workspace(grpc_channel)
    print_done(f"({ws_id[:8]})")
    
    print_step("Subscribing to workspace")
    try:
        ws_stub = workspace_services.WorkspaceServiceStub(grpc_channel)
        sub_req = workspace_services.WorkspaceStreamRequest(
            partial_eq_filter=[workspace_pb2.Workspace(
                key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id))
            )]
        )
        subscription = ws_stub.Subscribe(sub_req)
        try:
            next(subscription)
        except:
            pass
    except Exception as e:
        print(f"(warning: {e})")
    print_done()
    
    pods_needing_creation = [pn for pn, info in pod_locations.items() if info.get("needs_creation")]
    
    if pods_needing_creation:
        print_header("PHASE 3.6: CREATE POD STRUCTURES")
        
        config_stub = studio_services.InputsConfigServiceStub(grpc_channel)
        
        for pod_name in pods_needing_creation:
            pod_info = pod_locations[pod_name]
            c_idx, cp_idx, ap_idx = pod_info["location"]
            
            print_step(f"Creating {pod_name} at accessPod/{ap_idx}")
            
            pod_structure = {
                "tags": {"query": format_tag_query("Access-Pod", pod_name)},
                "inputs": {
                    "interfaces": []
                }
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
            
            for response in config_stub.SetSome(req, timeout=30):
                pass
            
            print_done()
    
    print_header("PHASE 3.7: PORT PROFILES")
    
    print_step("Building port profiles from CSV")
    profiles_map = {}
    for row in csv_data:
        profile_obj = build_profile_object(row)
        if profile_obj and profile_obj['name'] not in profiles_map:
            profiles_map[profile_obj['name']] = profile_obj
    
    print_done(f"({len(profiles_map)} unique profiles)")
    
    if profiles_map:
        print_step("Checking existing profiles in studio")
        existing_profiles = get_existing_port_profiles(existing_data)
        print_done(f"({len(existing_profiles)} existing)")
        
        new_profiles = set(profiles_map.keys()) - existing_profiles
        
        if new_profiles:
            print(f"\n    Creating {len(new_profiles)} new port profiles:")
            for pname in sorted(new_profiles)[:5]:
                print(f"      {pname}")
            if len(new_profiles) > 5:
                print(f"      ... and {len(new_profiles)-5} more")
            
            config_stub = studio_services.InputsConfigServiceStub(grpc_channel)
            profile_idx = len(existing_profiles)
            
            for profile_name in sorted(new_profiles):
                profile_obj = profiles_map[profile_name]
                
                path_values = [
                    "portProfiles", str(profile_idx)
                ]
                
                json_request = json.dumps({
                    "values": [{
                        "remove": False,
                        "inputs": json.dumps(profile_obj),
                        "key": {
                            "studioId": INTERFACE_STUDIO_ID,
                            "workspaceId": ws_id,
                            "path": {"values": path_values}
                        }
                    }]
                })
                
                req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
                
                for response in config_stub.SetSome(req, timeout=30):
                    pass
                
                profile_idx += 1
            
            print(f"    ✓ Created {len(new_profiles)} port profiles")
        else:
            print(f"    ✓ All profiles already exist")
    
    print_header("PHASE 4: WRITE INTERFACES")
    
    total_written = 0
    config_stub = studio_services.InputsConfigServiceStub(grpc_channel)
    
    for target_pod_name, interfaces in interfaces_by_pod.items():
        pod_info = pod_locations[target_pod_name]
        c_idx, cp_idx, ap_idx = pod_info["location"]
        existing_count = pod_info.get("total_interfaces", 0)
        
        print(f"\n  Writing {len(interfaces)} interfaces to {target_pod_name}")
        print(f"    Target: campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
        print(f"    Starting from interface index: {existing_count}")
        
        devices = {}
        for iface in interfaces:
            dev = iface["device_id"]
            devices[dev] = devices.get(dev, 0) + 1
        print(f"    Devices:")
        for dev_id, count in sorted(devices.items()):
            print(f"      {dev_id}: {count} interfaces")
        
        print(f"\n    Writing...")
        
        for idx, iface in enumerate(interfaces):
            interface_data = iface["interface_data"]
            
            interface_idx = existing_count + idx
            
            path_values = [
                "campus", str(c_idx), "inputs",
                "campusPod", str(cp_idx), "inputs",
                "accessPod", str(ap_idx), "inputs",
                "interfaces", str(interface_idx)
            ]
            
            json_request = json.dumps({
                "values": [{
                    "remove": False,
                    "inputs": json.dumps(interface_data),
                    "key": {
                        "studioId": INTERFACE_STUDIO_ID,
                        "workspaceId": ws_id,
                        "path": {"values": path_values}
                    }
                }]
            })
            
            req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
            
            for response in config_stub.SetSome(req, timeout=30):
                pass
            
            total_written += 1
        
        print(f"    ✓ Complete ({len(interfaces)} interfaces)")
    
    print(f"\n  Total: {total_written} interfaces written")
    
    print_header("PHASE 5: BUILD WORKSPACE")
    
    print_step("Triggering build")
    ws_stub = workspace_services.WorkspaceConfigServiceStub(grpc_channel)
    ws_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)), 
        request=1,
        request_params=workspace_pb2.RequestParams(request_id=wrappers.StringValue(value=str(uuid.uuid4())))
    )))
    print_done()
    
    print_header("SUCCESS")
    print(f"  Workspace: https://{CV_ADDR}/cv/provisioning/workspaces?ws={ws_id}")
    print(f"  Interfaces written: {total_written}")
    print(f"\n  The workspace has been automatically built!")
    print(f"  Open the workspace URL to review and submit changes.")
    
    if trunk_ports:
        print("\n" + "="*80)
        print(f"  ⚠ TRUNK PORTS SKIPPED ({len(trunk_ports)} total)")
        print("="*80)
        print(f"\n  These ports are listed as trunk ports and must be manually configured:")
        for trunk in trunk_ports[:10]:
            print(f"    • {trunk}")
        if len(trunk_ports) > 10:
            print(f"    ... and {len(trunk_ports)-10} more")
        print(f"\n  Please manually configure these trunk ports in CloudVision.")
        print("="*80)

if __name__ == "__main__":
    main()