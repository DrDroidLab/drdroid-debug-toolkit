import json
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


def simplify_network_map(raw_network_map: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simplify and clean up the raw network map data from otterize network-mapper.
    
    Args:
        raw_network_map: Raw JSON output from 'otterize network-mapper export --format json'
    
    Returns:
        Simplified network map structure
    """
    try:
        if not raw_network_map:
            return {}
        
        simplified = {
            'metadata': {
                'extraction_time': raw_network_map.get('metadata', {}).get('timestamp'),
                'version': raw_network_map.get('metadata', {}).get('version'),
                'cluster_info': raw_network_map.get('metadata', {}).get('cluster')
            },
            'services': [],
            'connections': [],
            'summary': {
                'total_services': 0,
                'total_connections': 0,
                'namespaces': set()
            }
        }
        
        # Extract services
        services = raw_network_map.get('services', [])
        for service in services:
            if not service:
                continue
                
            simplified_service = {
                'name': service.get('name'),
                'namespace': service.get('namespace', 'default'),
                'type': service.get('type'),
                'labels': service.get('labels', {}),
                'ports': service.get('ports', []),
                'selectors': service.get('selectors', {})
            }
            
            simplified['services'].append(simplified_service)
            simplified['summary']['namespaces'].add(simplified_service['namespace'])
        
        # Extract network connections
        connections = raw_network_map.get('connections', [])
        for connection in connections:
            if not connection:
                continue
                
            simplified_connection = {
                'source': {
                    'name': connection.get('source', {}).get('name'),
                    'namespace': connection.get('source', {}).get('namespace', 'default'),
                    'type': connection.get('source', {}).get('type')
                },
                'destination': {
                    'name': connection.get('destination', {}).get('name'),
                    'namespace': connection.get('destination', {}).get('namespace', 'default'),
                    'type': connection.get('destination', {}).get('type')
                },
                'protocol': connection.get('protocol', 'TCP'),
                'port': connection.get('port'),
                'is_internet': connection.get('is_internet', False)
            }
            
            simplified['connections'].append(simplified_connection)
        
        # Update summary
        simplified['summary']['total_services'] = len(simplified['services'])
        simplified['summary']['total_connections'] = len(simplified['connections'])
        simplified['summary']['namespaces'] = list(simplified['summary']['namespaces'])
        
        logger.info(f"Simplified network map: {simplified['summary']['total_services']} services, "
                   f"{simplified['summary']['total_connections']} connections across "
                   f"{len(simplified['summary']['namespaces'])} namespaces")
        
        return simplified
        
    except Exception as e:
        logger.error(f"Error simplifying network map: {e}")
        return {
            'error': str(e),
            'services': [],
            'connections': [],
            'summary': {'total_services': 0, 'total_connections': 0, 'namespaces': []}
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
        required_keys = ['services', 'connections', 'summary']
        for key in required_keys:
            if key not in network_map:
                logger.warning(f"Missing required key in network map: {key}")
                return False
        
        if not isinstance(network_map['services'], list):
            logger.warning("Network map 'services' should be a list")
            return False
            
        if not isinstance(network_map['connections'], list):
            logger.warning("Network map 'connections' should be a list")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error validating network map data: {e}")
        return False
