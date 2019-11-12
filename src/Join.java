
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Join {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().
                appName("ukpostcode").master("local[2]").getOrCreate();

        // Carregando o arquivo makerspace
        Dataset<Row> makerspace = session.read().
                option("header", "true"). // carregando com cabecalho
                option("inferSchema", "true"). // inferindo tipos
                csv("in/uk-makerspaces-identifiable-data.csv").
                as("tbl_makerspace");

        // Carregando o arquivo postcode
        Dataset<Row> postcode = session.read().
                option("header", "true").
                option("inferSchema", "true").
                csv("in/uk-postcode.csv").
                as("tbl_postcode");

        // Adicionando espaco na tabela postcode
        postcode = postcode.withColumn("Postcode",
                concat_ws("", col("Postcode"), lit(" "))).
                as("Postcode");

        postcode.select("postcode").show();

        // Olhando as primeiras linhas de cada tabela
//        makerspace.show(10);
//        postcode.show(10);
        makerspace.printSchema();
        postcode.printSchema();

        // Join
        // Condicao: a coluna de postcode da tabela makerspace
        // comeca com a coluna da tabela postcode
        Dataset<Row> joined = makerspace.join(postcode,
                makerspace.col("Postcode").
                        startsWith(postcode.col("Postcode")),
                "left_outer");

        // Olhando as primeiras 20 linhas
        joined.printSchema();
        joined.select("tbl_makerspace.Postcode",
                "tbl_postcode.Postcode").show(20);


        // parando a sessão
        session.stop();

    }
}