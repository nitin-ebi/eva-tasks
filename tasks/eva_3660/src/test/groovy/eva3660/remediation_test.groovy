package eva3660

import org.junit.jupiter.api.Test
import org.opencb.commons.utils.CryptoUtils

import java.util.regex.Matcher
import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.*

class RemediationApplicationIntegrationTest {
    private static String UPPERCASE_LARGE_SEQ = "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"


    @Test
    void testRegex() {
        Pattern pattern = Pattern.compile(RemediationApplication.REGEX_PATTERN)
        String[] matchingStrings = new String[] {
                buildVariantId("chr1", 77777777, "AcG", "AgT"),
                buildVariantId("chr1", 77777777, "", "AgT"),
                buildVariantId("chr1", 77777777, "AcG", ""),
                buildVariantId("chr1", 77777777, "555", "555"),
                buildVariantId("chr1", 77777777, "A", UPPERCASE_LARGE_SEQ),
                buildVariantId("chr1", 77777777, UPPERCASE_LARGE_SEQ, "A"),
                buildVariantId("chr1", 77777777, UPPERCASE_LARGE_SEQ, UPPERCASE_LARGE_SEQ),
                buildVariantId("chr_1", 77777777, "AcG", "AgT"),
                buildVariantId("chr_1", 77777777, "", "AgT"),
                buildVariantId("chr_1", 77777777, "AcG", ""),
                buildVariantId("chr_1", 77777777, "555", "555"),
                buildVariantId("chr_1", 77777777, "A", UPPERCASE_LARGE_SEQ),
                buildVariantId("chr_1", 77777777, UPPERCASE_LARGE_SEQ, "A"),
                buildVariantId("chr_1", 77777777, UPPERCASE_LARGE_SEQ, UPPERCASE_LARGE_SEQ)
        }
        for (String str : matchingStrings) {
            Matcher matcher = pattern.matcher(str)
            assertTrue(matcher.find(), "Expected string to match: " + str)
        }

        String[] notMatchingStrings = new String[] {
                buildVariantId("chr1", 77777777, "A", "G"),
                buildVariantId("chr1", 77777777, "A", ""),
                buildVariantId("chr1", 77777777, "", "g"),
                buildVariantId("chr1", 77777777, "A", "*"),
                buildVariantId("chr1", 77777777, "A", "<INS>"),
                buildVariantId("chr_1", 77777777, "A", "G"),
                buildVariantId("chr_1", 77777777, "A", ""),
                buildVariantId("chr_1", 77777777, "", "g"),
                buildVariantId("chr_1", 77777777, "A", "*"),
                buildVariantId("chr_1", 77777777, "A", "<INS>")
        }
        for (String str : notMatchingStrings) {
            Matcher matcher = pattern.matcher(str)
            assertFalse(matcher.find(), "Expected string not to match: " + str)
        }
    }

    String buildVariantId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome)
        builder.append("_")
        builder.append(start)
        builder.append("_")
        if (!reference.equals("-")) {
            if (reference.length() < 50) {
                builder.append(reference)
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(reference)))
            }
        }

        builder.append("_")
        if (!alternate.equals("-")) {
            if (alternate.length() < 50) {
                builder.append(alternate)
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(alternate)))
            }
        }
        return builder.toString()
    }

}