<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Exception;

use Flow\ETL\Exception\RuntimeException;

/**
 * Exception thrown when a WordPress core database operation fails.
 *
 * This exception is used when WordPress core functions like wp_insert_post(),
 * wp_update_term(), etc. return WP_Error objects.
 */
class WPAdapterDatabaseException extends RuntimeException
{
    /**
     * @var \WP_Error|null The WordPress error object if available
     */
    private ?\WP_Error $wpError;

    /**
     * Create a new exception from a WordPress error.
     *
     * @param \WP_Error $wpError The WordPress error object
     * @param string|null $message Optional custom message
     * @return self
     */
    public static function fromWPError(\WP_Error $wpError, ?string $message = null): self
    {
        $errorMessage = $message ?? 'WordPress operation failed: ' . $wpError->get_error_message();
        $exception = new self($errorMessage);
        $exception->wpError = $wpError;

        return $exception;
    }

    /**
     * Get the WordPress error object if available.
     *
     * @return \WP_Error|null
     */
    public function getWPError(): ?\WP_Error
    {
        return $this->wpError ?? null;
    }
}
