<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Exception;

use Flow\ETL\Exception\RuntimeException;

/**
 * Exception thrown when a loader doesn't detect any data to process.
 *
 * This exception is used when a loader expects to find data to process
 * but none is found, such as no taxonomy terms to update or no posts to process.
 */
class WPAdapterMissingDataException extends RuntimeException
{
    /**
     * @var string The entity type that was expected (post, term, user, etc.)
     */
    private string $entityType;

    /**
     * @var array|null Additional context about the missing data
     */
    private ?array $context;

    /**
     * Create a new exception for when no entities are found to process.
     *
     * @param string $entityType The type of entity that was expected (post, term, user, etc.)
     * @param string|null $message Optional custom message
     * @param array|null $context Additional context about the missing data
     * @return self
     */
    public static function noEntitiesFound(string $entityType, ?string $message = null, ?array $context = null): self
    {
        $errorMessage = $message ?? "No {$entityType} entities found to process";
        $exception = new self($errorMessage);
        $exception->entityType = $entityType;
        $exception->context = $context;

        return $exception;
    }

    /**
     * Get the entity type that was expected.
     *
     * @return string
     */
    public function getEntityType(): string
    {
        return $this->entityType;
    }

    /**
     * Get additional context about the missing data.
     *
     * @return array|null
     */
    public function getContext(): ?array
    {
        return $this->context;
    }
}
