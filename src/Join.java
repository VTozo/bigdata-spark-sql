import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class Join {

    static Dataset<Row> joinTables() {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com n threads
        SparkSession session = SparkSession.builder().
                appName("tde03").master("local[*]").getOrCreate();

        // Carregando o arquivo basico
        Dataset<Row> basico = session.read().
                option("header", "true"). // carregando com cabecalho
                option("inferSchema", "true"). // inferindo tipos
                csv("in/ommlbd_basico.csv").
                as("tb_basico");

        // Carregando o arquivo empresarial
        Dataset<Row> empresarial = session.read().
                option("header", "true").
                option("inferSchema", "true").
                csv("in/ommlbd_empresarial.csv").
                as("tb_empresarial");

        // Carregando o arquivo familiar
        Dataset<Row> familiar = session.read().
                option("header", "true").
                option("inferSchema", "true").
                csv("in/ommlbd_familiar.csv").
                as("tb_familiar");

        // Carregando o arquivo regional
        Dataset<Row> regional = session.read().
                option("header", "true").
                option("inferSchema", "true").
                csv("in/ommlbd_regional.csv").
                as("tb_regional");

        // Carregando o arquivo renda
        Dataset<Row> renda = session.read().
                option("header", "true").
                option("inferSchema", "true").
                csv("in/ommlbd_renda.csv").
                as("tb_renda");

        // Criando a tabela a partir da junção de todos os datasets
        Dataset<Row> joined = basico.
                join(empresarial, basico.col("HS_CPF").equalTo(
                        empresarial.col("HS_CPF")), "inner").
                join(familiar, basico.col("HS_CPF").equalTo(
                        familiar.col("HS_CPF")), "inner").
                join(regional, basico.col("HS_CPF").equalTo(
                        regional.col("HS_CPF")), "inner").
                join(renda, basico.col("HS_CPF").equalTo(
                        renda.col("HS_CPF")), "inner");

        return joined;

    }
}