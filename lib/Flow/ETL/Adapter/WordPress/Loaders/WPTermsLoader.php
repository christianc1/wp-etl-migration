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

final class WPTermsLoader implements Loader
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
            throw WPAdapterMissingDataException::noEntitiesFound('term', 'No terms found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->insertTerm($normalizedRow);
        }
    }

    public function insertTerm(Row | array $row, RowsNormalizer | null $normalizer = null): bool
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowsNormalizer) {
            $data = $normalizer->normalize(new Rows([$row]))[0];
        } else {
            $data = $row;
        }

        // Sanitize input data
        $sanitizedData = $this->sanitizeTermData($data);

        if (empty($sanitizedData['term.taxonomy'])) {
            throw WPAdapterDataException::missingRequiredData('term.taxonomy', $sanitizedData);
        }

        $taxonomy = $sanitizedData['term.taxonomy'];
        $name = $sanitizedData['term.name'] ?? '';
        $slug = $sanitizedData['term.slug'] ?? sanitize_title($name);

        // Verify taxonomy exists
        if (!taxonomy_exists($taxonomy)) {
            throw WPAdapterDataException::invalidDataFormat('term.taxonomy', 'existing taxonomy', [
                'taxonomy' => $taxonomy,
                'available_taxonomies' => get_taxonomies(),
            ]);
        }

        // Insert or update the term
        $termData = wp_insert_term(
            $name,
            $taxonomy,
            [
                'slug' => $slug,
                'description' => $sanitizedData['term.description'] ?? '',
            ]
        );

        if (is_wp_error($termData)) {
            // If term already exists, try to get its ID
            if ($termData->get_error_code() === 'term_exists') {
                $existingTerm = get_term_by('slug', $slug, $taxonomy);
                if ($existingTerm) {
                    $termData = ['term_id' => $existingTerm->term_id];
                } else {
                    throw WPAdapterDatabaseException::fromWPError($termData, "Failed to get existing term");
                }
            } else {
                throw WPAdapterDatabaseException::fromWPError($termData, "Failed to insert term");
            }
        }

        $termId = $termData['term_id'];

        // Process meta fields
        foreach ($sanitizedData as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $metaKey = sanitize_key(substr($key, 5)); // Remove 'meta.' prefix and sanitize
                update_term_meta($termId, $metaKey, $value);
            }
        }

        return true;
    }

    /**
     * Sanitize term data before insertion
     *
     * @param array $data Raw term data
     * @return array Sanitized term data
     */
    private function sanitizeTermData(array $data): array
    {
        $sanitized = [];

        // Sanitize taxonomy (required field)
        if (isset($data['term.taxonomy'])) {
            $sanitized['term.taxonomy'] = sanitize_key($data['term.taxonomy']);
        }

        // Sanitize term name
        if (isset($data['term.name'])) {
            $sanitized['term.name'] = sanitize_text_field($data['term.name']);
        }

        // Sanitize term slug
        if (isset($data['term.slug'])) {
            $sanitized['term.slug'] = sanitize_title($data['term.slug']);
        }

        // Sanitize term description (can contain limited HTML)
        if (isset($data['term.description'])) {
            $sanitized['term.description'] = wp_kses_post($data['term.description']);
        }

        // Pass through any meta fields for later processing
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                // For meta values, we'll sanitize based on the type
                if (is_numeric($value)) {
                    $sanitized[$key] = is_float($value + 0) ? (float)$value : (int)$value;
                } elseif (is_string($value)) {
                    $sanitized[$key] = sanitize_text_field($value);
                } elseif (is_array($value)) {
                    // For arrays, recursively sanitize each element
                    $sanitized[$key] = array_map(function($item) {
                        return is_string($item) ? sanitize_text_field($item) : $item;
                    }, $value);
                } else {
                    // For other types, pass through as is
                    $sanitized[$key] = $value;
                }
            } elseif (!isset($sanitized[$key])) {
                // Pass through any other fields that weren't explicitly sanitized
                $sanitized[$key] = $value;
            }
        }

        return $sanitized;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }
}
