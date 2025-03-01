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
 * WordPress Post Terms Loader for ETL operations
 *
 * This loader handles the association of terms with WordPress posts during ETL processes.
 * It supports data normalization, sanitization, and proper error handling for term relationships.
 *
 * @implements Loader
 */
final class WPPostTermsLoader implements Loader
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
     * Loads post terms from the provided rows
     *
     * @param Rows $rows The rows to process
     * @param FlowContext $context The flow context
     * @throws WPAdapterMissingDataException When no post terms are found to process
     * @return void
     */
    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('post_term', 'No post terms found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($rows as $row) {
            $this->setPostTerms($row, $normalizer);
        }
    }

    /**
     * Sets terms for a WordPress post
     *
     * @param Row|array<string, mixed> $row The row data to process
     * @param RowNormalizer|null $normalizer Optional normalizer for the data
     * @throws WPAdapterDataException When required data is missing or invalid
     * @throws WPAdapterDatabaseException When term assignment fails
     * @return bool True on successful term assignment
     */
    public function setPostTerms(Row | array $row, ?RowNormalizer $normalizer = null): bool
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowNormalizer) {
            $data = $normalizer->normalize($row);
        } else {
            $data = $row;
        }

        // Sanitize input data
        $sanitizedData = $this->sanitizePostTermsData($data);

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

        $termsUpdated = false;
        foreach ($sanitizedData as $key => $value) {
            if (str_starts_with($key, 'tax.')) {
                $taxonomy = sanitize_key(substr($key, 4)); // Remove 'tax.' prefix and sanitize

                // Handle array of terms or single term
                $terms = is_array($value) ? $value : [$value];

                // Filter out empty values
                $terms = array_filter($terms);

                if (empty($terms)) {
                    continue;
                }

                // Verify taxonomy exists
                if (!taxonomy_exists($taxonomy)) {
                    throw WPAdapterDataException::invalidDataFormat('taxonomy', 'existing taxonomy', [
                        'taxonomy' => $taxonomy,
                        'available_taxonomies' => get_taxonomies(),
                    ]);
                }

                // Sanitize term values
                $sanitizedTerms = $this->sanitizeTermValues($terms);

                $result = wp_set_object_terms($postId, $sanitizedTerms, $taxonomy);

                if (is_wp_error($result)) {
                    throw WPAdapterDatabaseException::fromWPError($result, "Failed to set terms for post ID {$postId} with taxonomy {$taxonomy}");
                }

                $termsUpdated = true;
            }
        }

        if (!$termsUpdated) {
            throw WPAdapterMissingDataException::noEntitiesFound('taxonomy_term', 'No taxonomy terms found to update', [
                'post_id' => $postId,
                'available_keys' => array_keys($sanitizedData)
            ]);
        }

        return true;
    }

    /**
     * Sanitizes post terms data before processing
     *
     * @param array<string, mixed> $data Raw post terms data
     * @return array<string, mixed> Sanitized post terms data
     */
    private function sanitizePostTermsData(array $data): array
    {
        $sanitized = [];

        // Sanitize post ID
        if (isset($data['post.ID'])) {
            $sanitized['post.ID'] = absint($data['post.ID']);
        }

        // Pass through taxonomy fields for later processing
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'tax.')) {
                $sanitized[$key] = $value; // We'll sanitize the individual terms later
            } elseif (!isset($sanitized[$key])) {
                // Pass through any other fields that weren't explicitly sanitized
                $sanitized[$key] = $value;
            }
        }

        return $sanitized;
    }

    /**
     * Sanitizes term values before setting them
     *
     * @param array<int|string> $terms Array of term values (can be term IDs, names, or slugs)
     * @return array<int|string> Sanitized term values
     */
    private function sanitizeTermValues(array $terms): array
    {
        return array_map(function($term) {
            if (is_numeric($term)) {
                // If it's a term ID, ensure it's an integer
                return absint($term);
            } elseif (is_string($term)) {
                // If it's a term name or slug, sanitize it
                return sanitize_text_field($term);
            } else {
                // For any other type, return as is
                return $term;
            }
        }, $terms);
    }

    /**
     * Sets the datetime format for the loader
     *
     * @param string $dateTimeFormat The format to use (should be a valid PHP date format)
     * @return self New instance with the updated format
     */
    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }
}
