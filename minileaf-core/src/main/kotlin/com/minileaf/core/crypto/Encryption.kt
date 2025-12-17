package com.minileaf.core.crypto

import com.minileaf.core.exception.StorageException
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * AES-256-GCM encryption utilities.
 */
object Encryption {
    private const val ALGORITHM = "AES/GCM/NoPadding"
    private const val TAG_LENGTH_BIT = 128
    private const val IV_LENGTH_BYTE = 12

    /**
     * Encrypts data using AES-256-GCM.
     * @param data The data to encrypt
     * @param key The 32-byte encryption key
     * @return The encrypted data with IV prepended
     */
    fun encrypt(data: ByteArray, key: ByteArray): ByteArray {
        require(key.size == 32) { "Encryption key must be 32 bytes for AES-256" }

        try {
            // Generate random IV
            val iv = ByteArray(IV_LENGTH_BYTE)
            SecureRandom().nextBytes(iv)

            // Create cipher
            val cipher = Cipher.getInstance(ALGORITHM)
            val keySpec = SecretKeySpec(key, "AES")
            val gcmSpec = GCMParameterSpec(TAG_LENGTH_BIT, iv)
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec)

            // Encrypt
            val encrypted = cipher.doFinal(data)

            // Prepend IV to encrypted data
            return iv + encrypted
        } catch (e: Exception) {
            throw StorageException("Encryption failed: ${e.message}", e)
        }
    }

    /**
     * Decrypts data using AES-256-GCM.
     * @param encryptedData The encrypted data with IV prepended
     * @param key The 32-byte encryption key
     * @return The decrypted data
     */
    fun decrypt(encryptedData: ByteArray, key: ByteArray): ByteArray {
        require(key.size == 32) { "Encryption key must be 32 bytes for AES-256" }
        require(encryptedData.size >= IV_LENGTH_BYTE) { "Invalid encrypted data: too short" }

        try {
            // Extract IV and encrypted data
            val iv = encryptedData.copyOfRange(0, IV_LENGTH_BYTE)
            val encrypted = encryptedData.copyOfRange(IV_LENGTH_BYTE, encryptedData.size)

            // Create cipher
            val cipher = Cipher.getInstance(ALGORITHM)
            val keySpec = SecretKeySpec(key, "AES")
            val gcmSpec = GCMParameterSpec(TAG_LENGTH_BIT, iv)
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

            // Decrypt
            return cipher.doFinal(encrypted)
        } catch (e: Exception) {
            throw StorageException("Decryption failed: ${e.message}", e)
        }
    }

    /**
     * Generates a random 32-byte encryption key.
     */
    fun generateKey(): ByteArray {
        val key = ByteArray(32)
        SecureRandom().nextBytes(key)
        return key
    }

    /**
     * Converts a hex string to a byte array (for key storage).
     */
    fun hexToBytes(hex: String): ByteArray {
        require(hex.length % 2 == 0) { "Hex string must have even length" }
        return hex.chunked(2)
            .map { it.toInt(16).toByte() }
            .toByteArray()
    }

    /**
     * Converts a byte array to a hex string (for key storage).
     */
    fun bytesToHex(bytes: ByteArray): String {
        return bytes.joinToString("") { "%02x".format(it) }
    }
}
