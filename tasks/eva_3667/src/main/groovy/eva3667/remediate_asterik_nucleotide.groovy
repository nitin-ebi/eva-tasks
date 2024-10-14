package eva3667

import groovy.cli.picocli.CliBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.util.stream.Collectors

def cli = new CliBuilder()
cli.inputLogsDir(args: 1, "Path to the logs directory", required: true)
cli.outputDir(args: 1, "Path to the output directory", required: true)

def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}

new AsteriskRemediationApplication(options.inputLogsDir, options.outputDir).process()

class AsteriskRemediationApplication {
    private static Logger logger = LoggerFactory.getLogger(RemediationApplication.class)
    private String inputLogsDir
    private String outputDir

    AsteriskRemediationApplication(String inputLogsDir, String outputDir) {
        this.inputLogsDir = inputLogsDir
        this.outputDir = outputDir
    }

    void process() {
        File dir = new File(inputLogsDir)
        if (dir.exists() && dir.isDirectory()) {
            // Filter out .err files
            List<String> filesList = Arrays.stream(dir.listFiles())
                    .filter(f -> f.isFile() && f.getName().endsWith(".out"))
                    .collect(Collectors.toList())

            if (!filesList.isEmpty()) {
                // go through each file in the dir
                for (File file : filesList) {
                    logger.info("Reading file: " + file.getName())
                    Set<String> variantIdSet = new ArrayList<>()
                    long count = 0l

                    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                        String line

                        while ((line = br.readLine()) != null) {
                            count++
                            // log after processing million lines
                            if (count % 1000000 == 0) {
                                logger.info("Processing {} line {}", file.getName(), count)
                            }

                            if ((line.contains("Variant does not contain any lowercase ref or alt") ||
                                    line.contains("Variant contains lowercase ref or alt"))
                                    && (line.contains("variant_ref: *") || line.contains("variant_alt: *"))) {

                                String[] idRefAlt = line.substring(line.indexOf("variant_id:")).split(",")
                                String variantId = idRefAlt[0].replace("variant_id:", "").trim()
                                String variantRef = idRefAlt[1].replace("variant_ref:", "").trim()
                                String variantAlt = idRefAlt[2].replace("variant_alt:", "").trim()

                                if (variantRef.length() >= 50 || variantAlt.length() >= 50) {
                                    logger.warn("The variant contains a large ref/alt. ref: {}, alt: {}", variantRef, variantAlt)
                                }

                                variantIdSet.add(variantId)
                            }
                        }

                        logger.info("Total variants in {} : {}", file.getName(), variantIdSet.size())
                        if (!variantIdSet.isEmpty()) {
                            File outputFile = new File(Paths.get(outputDir, file.getName().replace(".out", "") + "_asterisk.txt").toString())
                            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                                for (String value : variantIdSet) {
                                    writer.write(value)
                                    writer.newLine()
                                }
                                logger.info("Output file written successfully! {}", outputFile)
                            } catch (IOException e) {
                                e.printStackTrace()
                            }
                        } else {
                            logger.info("File {} does not contain any variant with asterisk", file.getName())
                        }
                    } catch (IOException e) {
                        e.printStackTrace()
                    }
                }
            } else {
                logger.error("No files found in the dir: {}" + inputLogsDir)
            }

        } else {
            logger.error("dir does not exist: {}" + inputLogsDir)
        }
    }
}
