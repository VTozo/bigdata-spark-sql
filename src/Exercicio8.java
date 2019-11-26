import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio8 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Filtro de clientes adimplentes
        Dataset<Row>datasetA = dataset.filter(col("TARGET").equalTo(0));

        // Filtro de clientes inadimplentes
        Dataset<Row>datasetI = dataset.filter(col("TARGET").equalTo(1));

        //Filtra apenas linhas com renda maior que 5000,00 reais
        datasetA = datasetA.filter(col("ESTIMATIVARENDA").$greater(5000));
        datasetI = datasetI.filter(col("ESTIMATIVARENDA").$greater(5000));


        //Filtro de clientes adimplentes e socios
        Dataset<Row>datasetAS = datasetA.filter(col("SOCIOEMPRESA").equalTo(1));

        //Filtro de clientes adimplentes e nao socios
        Dataset<Row>datasetAN = datasetA.filter(col("SOCIOEMPRESA").equalTo(0));

        //Filtro de clientes inadimplentes e socios
        Dataset<Row>datasetIS = datasetI.filter(col("SOCIOEMPRESA").equalTo(1));

        //Filtro de clientes inadimplentes e nao socios
        Dataset<Row>datasetIN = datasetI.filter(col("SOCIOEMPRESA").equalTo(0));

        // Conta o total de linhas restantes
        long countAS = datasetAS.count();
        long countAN = datasetAN.count();
        long countIS = datasetIS.count();
        long countIN = datasetIN.count();

        // Imprime o resultado
        System.out.println("Total ADIMPLENTES SOCIOS: " + countAS);
        System.out.println("Total ADIMPLENTES NAO SOCIOS: " + countAN);
        System.out.println("Total INADIMPLENTES SOCIOS: " + countIS);
        System.out.println("Total INADIMPLENTES NAO SOCIOS: " + countIN);

    }
}
