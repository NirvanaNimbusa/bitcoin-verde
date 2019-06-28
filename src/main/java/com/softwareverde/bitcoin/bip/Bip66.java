package com.softwareverde.bitcoin.bip;

import com.softwareverde.util.Util;

public class Bip66 {
    // Strict DER signatures -- https://github.com/bitcoin/bips/blob/master/bip-0066.mediawiki
    public static Boolean isEnabled(final Long blockHeight) {
        return (Util.coalesce(blockHeight, Long.MAX_VALUE) >= 363725L); // Block: 00000000000000000379EAA19DCE8C9B722D46AE6A57C2F1A988119488B50931
    }

    protected Bip66() { }
}
