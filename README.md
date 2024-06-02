# LAB6

## Implementación de Clúster EMR con AWS CLI

A continuación se va presentar los comandos para implementar un clúster EMR (Elastic MapReduce) en AWS utilizando la línea de comandos (CLI).

Inicia una cloud shell en aws, luego ingresa el siguiente comando:

```sh
aws emr create-cluster \
    --name "MyEMR_CLI" \
    --use-default-roles \
    --release-label emr-6.14.0 \
    --applications Name=Hadoop Name=Spark \
    --ec2-attributes KeyName=emr-key \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large \
                      InstanceGroupType=CORE,InstanceCount=1,InstanceType=m4.large \
                      InstanceGroupType=TASK,InstanceCount=1,InstanceType=m4.large \
    --use-default-roles

```
## Parámetros:

--name: Nombre del clúster.

--use-default-roles: Utiliza los roles predeterminados para EMR.

--release-label: Versión de EMR.

--applications: Lista de aplicaciones a instalar (Hadoop, Spark, etc.).

--ec2-attributes: Atributos EC2, incluyendo el par de claves (KeyName) para acceder a las instancias.

--instance-groups: Define los grupos de instancias.

--InstanceGroupType=MASTER: Define el nodo principal.

--InstanceGroupType=CORE: Define los nodos principales adicionales.

--InstanceGroupType=TASK: Define los nodos de tareas.

--instance-type: Tipo de instancia EC2 para el clúster.

--instance-count: Número de instancias en el clúster.

--log-uri: URI de S3 donde se almacenarán los logs del clúster.

![image](https://github.com/jrojasg1/LAB6/assets/60229862/b280a2db-fdef-406c-9cf3-5761ca7e99fc)

En la consola de AWS:

![image](https://github.com/jrojasg1/LAB6/assets/60229862/84ecc298-b47f-4c99-9773-4275d486eaff)


## Ejecución de Lab con datos en S3

Comando para ejecutar el programa:

```sh
python wordcount-mr.py s3://jrojasg1-lab-emr/data/datasets/gutenberg-small/*.txt -r hadoop --output-dir s3://jrojasg1-lab-emr/data/result_lab --conf-path mrjob.conf
```

Resultado en Consola:

![image](https://github.com/jrojasg1/LAB6/assets/60229862/222c6c64-8815-48ee-ba83-9b4776fcd144)

Resultados en S3:

![image](https://github.com/jrojasg1/LAB6/assets/60229862/5a07a53e-f84c-400c-9d72-e95901581d31)

![image](https://github.com/jrojasg1/LAB6/assets/60229862/5530ff84-d731-4105-add5-2de3a4b66fc7)
