## 1. Para los ids nulos ¿Qué sugieres hacer con ellos ?  
Resguardarlos en un objeto tipo dlq que después puede ser usado para revisar los casos e identificar las causas de la ausencia del dato y posiblemente aplicar las correcciones en los datos

## 2. Considerando las columnas name y company_id ¿Qué inconsistencias notas  y como las mitigas?
La inconsistencia mas evidente es la existencia de valores nulos. Es viable identificar los casos con este problema y resguardarlos para facilitar el analisis y la mitigación del problema. No es conveniente añadir un valor por defecto sin antes conocer el comportamiento del dato

## 3. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo  procedes?
- Para los campos de fechas se identificó que el formato de fecha no esta homologado, asumiendo que los campos son de fecha y los formatos posibles se han podido identificar, se procede a realizar la conversión a tipo date para el resguardo de los datos.
- para el campo status, existen algunos valores atipicos, sin embargo no es posible catalogarlos como errores sin conocer las definicion formal del dato

## 4. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones? 
- La generación de una dead letter queue que permita identificar los casos donde que pueden dar lugar a problemas de calidad o consistencia de la información.
- En el supuesto de que el proceso se ejecute periodicamente, agregar idempotencia al proceso de carga para permitir reprocesos sin riesgo de duplicar información