package com.collocation;

public class Sanitizer {
    
    public static String sanitize(String raw) {
        if (raw == null) return null;
        
        // 1. Trim whitespace
        String clean = raw.trim();
        
        // 2. Lowercase (normalization)
        clean = clean.toLowerCase();
        
        // 3. Remove punctuation from the START
        while (clean.length() > 0 && !Character.isLetterOrDigit(clean.charAt(0))) {
            clean = clean.substring(1);
        }

        // 4. Remove punctuation from the END
        while (clean.length() > 0 && !Character.isLetterOrDigit(clean.charAt(clean.length() - 1))) {
            clean = clean.substring(0, clean.length() - 1);
        }
        
        // 5. LENGTH CHECK (The Fix)
        // If the resulting word is less than 2 characters, discard it.
        // This removes 't', 'd', 's', 'a', 'i', '1', etc.
        if (clean.length() < 2) {
            return null; 
        }
        
        return clean;
    }
}
