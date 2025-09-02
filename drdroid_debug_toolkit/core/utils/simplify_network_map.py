import json
import logging
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)


def extract_service_key(workload: dict, namespace: str) -> str:
    """Extract a unique service key from workload and namespace."""
    name = workload.get("name", "")
    kind = workload.get("kind", "")
    return f"{namespace}/{name}" if namespace else name


def extract_target_key(target: dict, source_namespace: str) -> str:
    """Extract a unique target key from a target object."""
    if "kubernetes" in target:
        k8s = target["kubernetes"]
        name = k8s.get("name", "")
        # Handle cross-namespace references (e.g., "apm-server-apm-server.tracing")
        if "." in name:
            service_name, target_namespace = name.split(".", 1)
            return f"{target_namespace}/{service_name}"
        else:
            # Same namespace as source
            return f"{source_namespace}/{name}"
    elif "service" in target:
        service_name = target["service"].get("name", "")
        # Services like "kubernetes.default" are typically cluster-wide
        if "." in service_name:
            return service_name
        else:
            return f"{source_namespace}/{service_name}"
    return ""


def build_service_map_from_client_intents(data: List[dict]) -> Dict[str, dict]:
    """Build a simplified service map from the ClientIntents data."""
    
    # Track upstream relationships (who each service calls)
    upstream_map = defaultdict(set)
    
    # Track all services we've seen
    all_services = set()
    
    for intent in data:
        if intent.get("kind") != "ClientIntents":
            continue
            
        metadata = intent.get("metadata", {})
        spec = intent.get("spec", {})
        
        namespace = metadata.get("namespace", "")
        workload = spec.get("workload", {})
        targets = spec.get("targets", [])
        
        # Extract source service
        source_key = extract_service_key(workload, namespace)
        if not source_key:
            continue
            
        all_services.add(source_key)
        
        # Extract target services (upstream dependencies)
        for target in targets:
            target_key = extract_target_key(target, namespace)
            if target_key:
                upstream_map[source_key].add(target_key)
                all_services.add(target_key)
    
    # Build downstream relationships (who calls each service)
    downstream_map = defaultdict(set)
    for source, upstreams in upstream_map.items():
        for upstream in upstreams:
            downstream_map[upstream].add(source)
    
    # Create the final simplified structure
    service_map = {}
    for service in sorted(all_services):
        # Parse namespace and name
        if "/" in service:
            namespace, name = service.split("/", 1)
        else:
            namespace = ""
            name = service
            
        service_map[service] = {
            "name": name,
            "namespace": namespace,
            "upstream": sorted(list(upstream_map[service])),
            "downstream": sorted(list(downstream_map[service]))
        }
    
    return service_map


def simplify_network_map(raw_network_map: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simplify and clean up ClientIntents data into service relationship map.
    
    Args:
        raw_network_map: List of ClientIntents objects from network mapping tools
    
    Returns:
        Simplified network map structure with service relationships
    """
    try:
        if not raw_network_map:
            return {}
        
        # Process ClientIntents format
        service_map = build_service_map_from_client_intents(raw_network_map)
        
        # Calculate summary statistics
        total_upstream = sum(len(service["upstream"]) for service in service_map.values())
        total_downstream = sum(len(service["downstream"]) for service in service_map.values())
        namespaces = set(service["namespace"] for service in service_map.values() if service["namespace"])
        
        return {
            'metadata': {
                'total_services': len(service_map),
                'total_upstream_connections': total_upstream,
                'total_downstream_connections': total_downstream,
                'namespaces': sorted(list(namespaces)),
                'description': 'Simplified service relationship map from ClientIntents'
            },
            'services': service_map
        }
        
    except Exception as e:
        logger.error(f"Error simplifying network map: {e}")
        return {
            'error': str(e),
            'metadata': {
                'total_services': 0,
                'total_upstream_connections': 0,
                'total_downstream_connections': 0,
                'namespaces': [],
                'description': 'Error processing ClientIntents data'
            },
            'services': {}
        }


def validate_network_map_data(network_map: Dict[str, Any]) -> bool:
    """
    Validate that network map data has the expected structure.
    
    Args:
        network_map: Network map data to validate
    
    Returns:
        True if valid, False otherwise
    """
    try:
        # Check for required metadata
        if 'metadata' not in network_map:
            logger.warning("Missing required key in network map: metadata")
            return False
        
        # Check for services key
        if 'services' not in network_map:
            logger.warning("Missing required key in network map: services")
            return False
        
        # Services should be a dict with service mappings
        if not isinstance(network_map['services'], dict):
            logger.warning("Network map 'services' should be a dict")
            return False
        
        # Validate service structure
        for service_key, service_data in network_map['services'].items():
            required_service_keys = ['name', 'namespace', 'upstream', 'downstream']
            for key in required_service_keys:
                if key not in service_data:
                    logger.warning(f"Missing required key '{key}' in service '{service_key}'")
                    return False
            
            # Validate that upstream and downstream are lists
            if not isinstance(service_data['upstream'], list):
                logger.warning(f"Service '{service_key}' upstream should be a list")
                return False
            
            if not isinstance(service_data['downstream'], list):
                logger.warning(f"Service '{service_key}' downstream should be a list")
                return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error validating network map data: {e}")
        return False
