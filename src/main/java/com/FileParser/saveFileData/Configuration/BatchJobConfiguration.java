import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.ArrayFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.InputStreamResource;

import java.io.IOException;
import java.io.InputStream;

@Configuration
@EnableBatchProcessing
public class CsvToDbBatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private UserRepository userRepository;  // Your JPA repository

    // Bean to hold the InputStream (injected at runtime)
    private InputStream csvInputStream;

    public void setCsvInputStream(InputStream csvInputStream) {
        this.csvInputStream = csvInputStream;
    }

    @Bean
    public FlatFileItemReader<String[]> csvReader() {
        FlatFileItemReader<String[]> reader = new FlatFileItemReader<>();
        reader.setResource(new InputStreamResource(csvInputStream));  // Use the InputStream
        reader.setLinesToSkip(1);  // Skip header row
        reader.setLineMapper(new DefaultLineMapper<String[]>() {{
            setLineTokenizer(new DelimitedLineTokenizer());  // Assumes comma-delimited; adjust if needed
            setFieldSetMapper(new ArrayFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public ItemProcessor<String[], User> csvProcessor() {
        return row -> {
            // Apply filter: Skip malformed data (e.g., invalid email or empty fields)
            if (row.length < 4 || row[1] == null || row[1].trim().isEmpty() || !row[1].contains("@")) {
                return null;  // Skip this row
            }
            User user = new User();
            user.setEmail(row[1].trim());
            user.setPhoneno(row[2].trim());
            user.setDept(row[3].trim());
            return user;
        };
    }

    @Bean
    public RepositoryItemWriter<User> csvWriter() {
        RepositoryItemWriter<User> writer = new RepositoryItemWriter<>();
        writer.setRepository(userRepository);
        writer.setMethodName("save");  // Calls userRepository.save(user)
        return writer;
    }

    @Bean
    public Step csvStep() {
        return stepBuilderFactory.get("csvStep")
                .<String[], User>chunk(10)  // Process in chunks of 10 for efficiency
                .reader(csvReader())
                .processor(csvProcessor())
                .writer(csvWriter())
                .faultTolerant()  // Enable error handling
                .skip(Exception.class)  // Skip rows that cause exceptions (e.g., DB errors)
                .skipLimit(100)  // Max skips before failing the job
                .build();
    }

    @Bean
    public Job csvToDbJob() {
        return jobBuilderFactory.get("csvToDbJob")
                .incrementer(new RunIdIncrementer())  // For unique job instances
                .start(csvStep())
                .build();
    }
}

}
