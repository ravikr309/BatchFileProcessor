
@RestController
@RequestMapping("/api/v1")
public class CSVFileParserController {

    @Autowired
    private CSVFileParserService csvFileParserService;

    @PostMapping("/csvfileupdate")
    public ResponseEntity<?> updateDbFromCSV(@RequestParam("File") MultiPartFile file)
    {
        if(file.isEmpty){
            return ;
        }
        else{
            boolean updatedone = csvFileParserService.updateCSVFiletoDb(file.getInputStream());
            if(updatedone){
                return "Done";
            }
            else{
                return "Failed";
            }


        }
    }


}
