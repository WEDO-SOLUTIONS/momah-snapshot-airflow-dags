from airflow.plugins_manager import AirflowPlugin

class UrbiProPlugin(AirflowPlugin):
    name = "urbi_pro_plugin"
    hooks = [] # Hooks are automatically discovered
    operators = []
    # Add other plugin components here if needed