import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

@Service
public class YourService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job csvToDbJob;

    @Autowired
    private CsvToDbBatchConfig batchConfig;  //This is the configuration class  // To set the InputStream

    public boolean updateCSVFiletoDb(InputStream csvInputStream) throws IOException, CSVException {
        try {
            // Set the InputStream in the config
            batchConfig.setCsvInputStream(csvInputStream);

            // Launch the job with unique parameters (e.g., timestamp)
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(csvToDbJob, jobParameters);
            return true;  // Or check job execution status for more detail
        } catch (Exception e) {
            // Log the exception properly (e.g., using SLF4J)
            // You can throw a custom exception or return false
            throw new RuntimeException("Batch job failed", e);
        }
    }
}