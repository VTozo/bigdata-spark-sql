import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio6 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Remove linhas com IDH -9999
        dataset = dataset.filter(col("IDHMUNICIPIO").notEqual("-9999"));

        // Cria coluna com o IDH dividido por 10 (parte inteira) para criar as faixas
        dataset = dataset.withColumn("IDHRANGE", dataset.col("IDHMUNICIPIO").divide(10).cast("int"));

        // Calcula o total de itens, para a porcentagem
        double countTotal = dataset.count();

        // Agrupa por faixa
        RelationalGroupedDataset idhrange = dataset.groupBy(col("IDHRANGE"));

        // Para cada faixa printa a descrição e a porcentagem
        idhrange.count().foreach(v -> System.out.println(
                Integer.parseInt(v.get(0).toString()) * 10 + " a " +
                        (Integer.parseInt(v.get(0).toString()) + 1) * 10 + " : " +
                        Integer.parseInt(v.get(1).toString()) / countTotal * 100 + "%"
        ));

    }
}
