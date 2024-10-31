package eva3660

class ValuesForNormalisation {

    final int position
    final String reference
    final String alternate
    final String mafAllele
    final List<String> secondaryAlternates

    ValuesForNormalisation(int position, String reference, String alternate, String mafAllele,
                           List<String> secondaryAlternates) {
        this.position = position
        this.reference = reference
        this.alternate = alternate
        this.mafAllele = mafAllele
        this.secondaryAlternates = secondaryAlternates
    }

}
