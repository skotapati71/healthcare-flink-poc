from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import TableDescriptor, Schema, DataTypes

def main():
    settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(settings)

    field_names = ['name', 'age', 'gender', 'bloodType',
                   'medicalCondition', 'admissionDate', 'doctor', 'hospital', 'insuranceProvider',
                   'BilledAmount','roomNumber','admissionType','dischargeDate', 'medication', 'testResults']
    field_types = [DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING() , DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),]

    schema = Schema.new_builder().from_fields(field_names, field_types).build()
    print(schema)

    # create table
    table_env.create_table(
        'health_data_sm',
        TableDescriptor.for_connector('filesystem')
        .schema(schema)
        .option('path', 'input_data')
        .format('csv')
        .build()
    )

    print(table_env.list_tables())
    health_data_table = table_env.from_path('health_data_sm')
    # print(health_data_table.to_pandas().head())

    # print distinct test results
    print(health_data_table.select(health_data_table.testResults).distinct().to_pandas())

    # print rows with abnormal test results
    abnormal_test_results = health_data_table.filter(health_data_table.testResults == 'Abnormal')
    print(abnormal_test_results.to_pandas())

    # explain abnormal testreults plan
    print(abnormal_test_results.explain())

if __name__ == "__main__" :
    main()

