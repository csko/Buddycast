package recommendation;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class SimilarityMatrixFromFile {

    private static final String similarityFileName = "test/ua_norm.base_similarities_cofe_cos_cent";
    private static float[][] similarities = null;
    private static SimilarityMatrixFromFile instance = null;

    public double computeSimilarity(int i, int j) {
        // read similarity from right part of the similarity matrix
        return (j >= i) ? similarities[i][j - i] : similarities[j][i - j];
    }

    private SimilarityMatrixFromFile() throws Exception {
        ObjectInputStream similarityInput = new ObjectInputStream(new BufferedInputStream(new FileInputStream(similarityFileName)));
        similarities = (float[][]) similarityInput.readObject();
        similarityInput.close();
    }

    public static SimilarityMatrixFromFile getInstance() throws Exception {
        if (instance == null) {
            instance = new SimilarityMatrixFromFile();
        }
        return instance;
    }
}
