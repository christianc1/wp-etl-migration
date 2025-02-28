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
    RowsNormalizer
};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPPostTermsLoader implements Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    public function __construct(
        private readonly array $config = []
    ) {
    }

    public function create_normalizer(FlowContext $context): RowsNormalizer
    {
        return new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('post_term', 'No post terms found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->setPostTerms($normalizedRow);
        }
    }

    public function setPostTerms(Row | array $row, RowsNormalizer | null $normalizer = null): bool
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowsNormalizer) {
            $data = $normalizer->normalize(new Rows([$row]))[0];
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
     * Sanitize post terms data before processing
     *
     * @param array $data Raw post terms data
     * @return array Sanitized post terms data
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
     * Sanitize term values before setting them
     *
     * @param array $terms Array of term values (can be term IDs, names, or slugs)
     * @return array Sanitized term values
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

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }
}
