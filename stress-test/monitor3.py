from rich import print
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from jmxquery import JMXConnection, JMXQuery
import time

# Conectar al servidor JMX
jmxConnection = JMXConnection("service:jmx:rmi:///jndi/rmi://localhost:9997/jmxrmi")

# Definir consultas para métricas
jmxQuery_messages_in = [JMXQuery("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
                                metric_name="MessagesInPerSec")]
jmxQuery_messages_out = [JMXQuery("kafka.server:type=BrokerTopicMetrics,name=MessagesOutPerSec",
                                 metric_name="MessagesOutPerSec")]
jmxQuery_consumer_lag = [JMXQuery("kafka.consumer:type=ConsumerLag,clientId=*,topic=*,partition=*",
                                 metric_name="ConsumerLag")]
jmxQuery_cpu_usage = [JMXQuery("java.lang:type=OperatingSystem",
                              metric_name="SystemCpuLoad")]

def get_metrics():
    try:
        metrics_in = jmxConnection.query(jmxQuery_messages_in)
        in_value = metrics_in[0].value if metrics_in else "N/A"
        metrics_out = jmxConnection.query(jmxQuery_messages_out)
        out_value = metrics_out[0].value if metrics_out else "N/A"
        metrics_lag = jmxConnection.query(jmxQuery_consumer_lag)
        lag_value = metrics_lag[0].value if metrics_lag else "N/A"
        metrics_cpu = jmxConnection.query(jmxQuery_cpu_usage)
        cpu_value = metrics_cpu[0].value if metrics_cpu else "N/A"
        return in_value, out_value, lag_value, cpu_value
    except Exception as e:
        return "Error", "Error", "Error", "Error"

def create_table(metrics):
    table = Table(title="Métricas de Kafka", title_style="bold magenta")
    table.add_column("Métrica", justify="left", style="cyan")
    table.add_column("Valor", justify="right", style="green")

    in_value, out_value, lag_value, cpu_value = metrics

    # Función auxiliar para determinar el estilo según el valor
    def get_style(value, threshold, normal="green", warning="red"):
        if value in ["N/A", "Error"]:  # Si el valor no es numérico
            return "yellow"
        try:
            return normal if float(value) < threshold else warning
        except ValueError:  # Por si hay otros errores de conversión
            return "yellow"

    # Aplicar estilos según umbrales
    in_style = get_style(in_value, 1000)
    out_style = get_style(out_value, 1000)
    lag_style = get_style(lag_value, 100)
    cpu_style = get_style(cpu_value, 0.8)

    # Agregar filas a la tabla con los estilos correspondientes
    table.add_row("Mensajes Entrantes por Seg", str(in_value), style=in_style)
    table.add_row("Mensajes Salientes por Seg", str(out_value), style=out_style)
    table.add_row("Retraso del Consumidor", str(lag_value), style=lag_style)
    table.add_row("Uso de CPU", str(cpu_value), style=cpu_style)

    return table

# Mostrar métricas en vivo
with Live(refresh_per_second=1) as live:
    while True:
        metrics = get_metrics()
        table = create_table(metrics)
        panel = Panel(table, title="Monitor de Kafka", border_style="blue")
        live.update(panel)
        time.sleep(1)
