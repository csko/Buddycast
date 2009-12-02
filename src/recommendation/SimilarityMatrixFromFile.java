package recommendation;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class SimilarityMatrixFromFile {

    private static String similarityFileName;// = "test/ua_norm.base_similarities_cofe_cos_cent";
    private static float[][] similarities = null;
    private static SimilarityMatrixFromFile instance = null;

    public double computeSimilarity(int i, int j) {
        // read similarity from right part of the similarity matrix
        return (j >= i) ? similarities[i][j - i] : similarities[j][i - j];
    }

    private SimilarityMatrixFromFile() {
        ObjectInputStream similarityInput = null;
        try {
            similarityInput = new ObjectInputStream(new BufferedInputStream(new FileInputStream(similarityFileName)));
            similarities = (float[][]) similarityInput.readObject();
            similarityInput.close();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                similarityInput.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static SimilarityMatrixFromFile getInstance() {
        if (instance == null) {
            instance = new SimilarityMatrixFromFile();
        }
        return instance;
    }

    public static void setSimilarityFileName(String similarityFileName) {
        SimilarityMatrixFromFile.similarityFileName = similarityFileName;
    }

}
