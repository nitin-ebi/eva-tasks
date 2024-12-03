package eva3660

/**
 * Simple class to encapsulate values from VariantDocument that must be updated during normalisation.
 */
class ValuesForNormalisation {

    final int start
    final int end
    final int length
    final String reference
    final String alternate
    final List<String> secondaryAlternates

    ValuesForNormalisation(int start, int end, int length, String reference, String alternate,
                           List<String> secondaryAlternates) {
        this.start = start
        this.end = end
        this.length = length
        this.reference = reference
        this.alternate = alternate
        this.secondaryAlternates = secondaryAlternates
    }

    @Override
    boolean equals(Object other) {
        if (!other instanceof ValuesForNormalisation) return false
        other = (ValuesForNormalisation)other
        return this.start == other.start && this.end == other.end && this.length == other.length
                && this.reference.equals(other.reference) && this.alternate.equals(other.alternate)
                && this.secondaryAlternates.equals(other.secondaryAlternates)
    }

    @Override
    String toString() {
        return "ValuesForNormalisation(start=$start, end=$end, length=$length, reference=$reference," +
                " alternate=$alternate, secondaryAlternates=${secondaryAlternates.toString()}"
    }

}
