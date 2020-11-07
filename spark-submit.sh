#!/usr/bin/env bash
home_dir=/efs/home/ps532392/dev/product/case_ibp
if [[ $# -lt 2 ]]; then
	echo -e "\n================================================================= \n"
	echo "Minimum one arguments required!!!!"
    echo -e "\nUSAGE: ${home_dir}/bin/spark-submit.sh <SourceSystem> <sourcedata_regionname>"
    echo -e "\nOPTIONAL USAGE: ${home_dir}/bin/spark-submit.sh <SourceSystem> <sourcedata_regionname> <table_names>"
	echo
	echo " SourceSystem: 												 "
	echo " 		1.rtduet	 	 "
	echo " 		2.rtduet_cdp	 	 "
	echo " Region: 												 "
	echo " 		1.NA	 	 "
	echo " 		2.EMEA	 	 "
	echo
	echo " TableName:"
	echo " 		Optional argument. if table name is passed an argument, data will be ingested for that particular table only"
	echo "================================================================"
    exit 1
fi

source_system=$1
region=$2
table_names=$3
env=dev
server=drona

export PYTHON_EGG_CACHE=./myeggs
kinit ${USER}@AP.CORP.CARGILL.COM -k -t /efs/home/${USER}/${USER}.keytab

timestamp=$(date +%D/%T)
log_time=$(echo ${timestamp} | sed -e "s/\//_/g"| sed -e "s/:/_/g")

log_home=${home_dir}/logs
log_temp=${home_dir}/logs/spark_${source_system}_runstate.log
log_dir=${log_home}/spark_${source_system}_${log_time}.log

echo 'home directory is' ${home_dir}
echo 'Logging directory is' ${log_dir}


#Spark properties
master=yarn
deployment_mode=cluster
executor_memory=8G
num_of_executors=4
cores_per_executor=5

#Application properties
class_name=org.cargill.ibp.IBPMain
property_file=${home_dir}/config/spark_ibp_conf.properties
jar_directory=${home_dir}/lib/CASC_IBP-1.0-SNAPSHOT.jar

common_libs=$(echo ${home_dir}/lib/common/*.jar | tr ' ' ',')
echo "Executing spark submit ............."
spark2-submit --master ${master} --deploy-mode ${deployment_mode} --packages com.crealytics:spark-excel_2.11:0.11.1 --executor-memory ${executor_memory} --num-executors ${num_of_executors} --executor-cores ${cores_per_executor} --properties-file ${property_file} --jars ${common_libs} --class ${class_name} ${jar_directory} ${source_system} ${region} ${table_names} 2>&1 | tee "$log_temp"

application_id="$(grep "Submitted application" ${log_temp}|grep -o '\bapplication_\w*')"
yarn logs -applicationId ${application_id} >> ${log_dir}
echo "Find log file for the applicationId $application_id => $log_dir"

IMPALA_SHELL="impala-shell --var=db_prefix=$env -k --ssl -i ${server}-impala.cargill.com"
BEELINE_SHELL="beeline --showHeader=false --outputformat=tsv2 -u 'jdbc:hive2://${server}-hive.cargill.com:10000/default;principal=hive/_HOST@NA.CORP.CARGILL.COM;ssl=true;' -d org.apache.hive.jdbc.HiveDriver"
databaseQuery="select target_database from ${env}_product_casc_ibp.ibp_config where source_system='$source_system' limit 1;"
database=`${IMPALA_SHELL} -B -q "$databaseQuery"`
echo "database name $database"
query="use ${database};show tables"
echo "table query $query"
tableList=`${BEELINE_SHELL} -e "$query"`
for table_name in ${tableList};
   do
     if  [[ ${table_name} == *_intermediate ]];
        then
            echo -e "dropping hive temp table => $table_name"
            drop_query="DROP TABLE $database.$table_name;"
            `${BEELINE_SHELL} -e "$drop_query"`
            echo -e "Invalidate metadata for table => $table_name"
            invalidate_query="INVALIDATE METADATA ${database}.$table_name;"
            `${IMPALA_SHELL} -B -q "$invalidate_query"`
       fi
   done


if grep -q "finished with failed status" "$log_temp";
    then
        echo "IBP Spark submit has failed "
        exit 1;
else
      echo "IBP Spark submit run successfully "
      exit 0;
 fi