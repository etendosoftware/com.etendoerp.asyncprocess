from jmxquery import JMXConnection, JMXQuery

# Conectar al servidor JMX (ajusta la URL según tu configuración)
jmxConnection = JMXConnection("service:jmx:rmi:///jndi/rmi://localhost:9997/jmxrmi")

# Definir la consulta para métricas de clúster Kafka
jmxQuery = [JMXQuery("kafka.cluster:type=*,name=*,topic=*,partition=*",
                     metric_name="kafka_cluster_{type}_{name}",
                     metric_labels={"topic": "{topic}", "partition": "{partition}"})]

# Ejecutar la consulta y mostrar los resultados
metrics = jmxConnection.query(jmxQuery)
for metric in metrics:
    print(f"{metric.metric_name}<{metric.metric_labels}> == {metric.value}")
