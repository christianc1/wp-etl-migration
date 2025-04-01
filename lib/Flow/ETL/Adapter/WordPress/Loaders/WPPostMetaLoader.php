<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Loaders;

use Flow\ETL\Adapter\WordPress\Exception\{
	WPAdapterDatabaseException,
	WPAdapterDataException,
	WPAdapterMissingDataException
};
use Flow\ETL\Adapter\WordPress\Normalizers\{
    EntryNormalizer,
    RowNormalizer
};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

/**
 * WordPress Post Meta Loader for ETL operations
 *
 * This loader handles the insertion and updating of WordPress post meta data during ETL processes.
 * It supports data normalization, sanitization, and proper error handling for post meta operations.
 *
 * @implements Loader
 */
final class WPPostMetaLoader implements Loader
{
    /**
     * @var string The format to use for datetime values
     */
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    /**
     * Constructor
     *
     * @param array<string, mixed> $config Configuration array for the loader
     */
    public function __construct(
        private readonly array $config = []
    ) {
    }

    /**
     * Creates a row normalizer instance
     *
     * @param FlowContext $context The flow context
     * @return RowNormalizer The created normalizer
     */
    public function create_normalizer(FlowContext $context): RowNormalizer
    {
        return new RowNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    /**
     * Loads post meta from the provided rows
     *
     * @param Rows $rows The rows to process
     * @param FlowContext $context The flow context
     * @throws WPAdapterMissingDataException When no post meta is found to process
     * @return void
     */
    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('post_meta', 'No post meta found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($rows as $row) {
            $this->insertPostMeta($row, $normalizer);
        }
    }

    /**
     * Inserts or updates post meta for a WordPress post
     *
     * @param Row|array<string, mixed> $row The row data to process
     * @param RowNormalizer|null $normalizer Optional normalizer for the data
     * @throws WPAdapterDataException When required data is missing or invalid
     * @throws WPAdapterDatabaseException When meta update fails
     * @return bool True on successful meta update
     */
    public function insertPostMeta(Row | array $row, ?RowNormalizer $normalizer = null): bool
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowNormalizer) {
            $data = $normalizer->normalize($row);
        } else {
            $data = $row;
        }

        // Sanitize input data
        $sanitizedData = $this->sanitizePostMetaData($data);

        if (empty($sanitizedData['post.ID'])) {
            throw WPAdapterDataException::missingRequiredData('post.ID', $sanitizedData);
        }

        $postId = (int) $sanitizedData['post.ID'];

        // Verify post exists
        if (!get_post($postId)) {
            throw WPAdapterDataException::invalidDataFormat('post.ID', 'existing post ID', [
                'post_id' => $postId
            ]);
        }

        // Process all meta.* entries
        $metaUpdated = false;

        foreach ($sanitizedData as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $metaKey = sanitize_key(substr($key, 5)); // Remove 'meta.' prefix and sanitize

                // Sanitize meta value based on type
                $sanitizedValue = $this->sanitizeMetaValue($value);

                update_post_meta(absint($postId), $metaKey, $sanitizedValue);
                $metaUpdated = true;
            }
        }

        if (!$metaUpdated) {
            throw WPAdapterMissingDataException::noEntitiesFound('post_meta', 'No meta fields found to update', [
                'post_id' => $postId,
                'available_keys' => array_keys($sanitizedData)
            ]);
        }

        return true;
    }

    /**
     * Sanitize post meta data before processing
     *
     * @param array $data Raw post meta data
     * @return array Sanitized post meta data
     */
    private function sanitizePostMetaData(array $data): array
    {
        $sanitized = [];

        // Sanitize post ID
        if (isset($data['post.ID'])) {
            $sanitized['post.ID'] = absint($data['post.ID']);
        }

        // Pass through meta fields for later processing
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $sanitized[$key] = $value; // We'll sanitize the meta values later
            } elseif (!isset($sanitized[$key])) {
                // Pass through any other fields that weren't explicitly sanitized
                $sanitized[$key] = $value;
            }
        }

        return $sanitized;
    }

    /**
     * Sanitize a meta value based on its type
     *
     * @param mixed $value The meta value to sanitize
     * @return mixed Sanitized meta value
     */
    private function sanitizeMetaValue($value)
    {
        if (is_numeric($value)) {
            // If it's a numeric value, preserve its type (int or float)
            return is_float($value + 0) ? (float)$value : (int)$value;
        } elseif (is_string($value)) {
            // For strings, use appropriate sanitization
            if (strpos($value, '<') !== false && strpos($value, '>') !== false) {
                // If it looks like HTML, use wp_kses_post
                return wp_kses_post($value);
            } else {
                // Otherwise use standard text sanitization
                return sanitize_text_field($value);
            }
        } elseif (is_array($value)) {
            // For arrays, recursively sanitize each element
            return array_map([$this, 'sanitizeMetaValue'], $value);
        } elseif (is_bool($value)) {
            // Preserve boolean values
            return $value;
        } elseif (is_null($value)) {
            // Preserve null values
            return $value;
        } else {
            // For any other type, return as is
            return $value;
        }
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }
}
