<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPTermsLoader implements Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    public function __construct(
        private readonly array $config = []
    ) {
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $normalizer = new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->insertTerm($normalizedRow);
        }
    }

    private function insertTerm(array $data): bool
    {
        if (empty($data['term.taxonomy'])) {
            throw new RuntimeException('Term taxonomy is required');
        }

        $taxonomy = $data['term.taxonomy'];
        $name = $data['term.name'] ?? '';
        $slug = $data['term.slug'] ?? sanitize_title($name);

        // Verify taxonomy exists
        if (!taxonomy_exists($taxonomy)) {
            throw new RuntimeException("Taxonomy '{$taxonomy}' does not exist");
        }

        // Insert or update the term
        $termData = wp_insert_term(
            $name,
            $taxonomy,
            [
                'slug' => $slug,
                'description' => $data['term.description'] ?? '',
            ]
        );

        if (is_wp_error($termData)) {
            // If term already exists, try to get its ID
            if ($termData->get_error_code() === 'term_exists') {
                $existingTerm = get_term_by('slug', $slug, $taxonomy);
                if ($existingTerm) {
                    $termData = ['term_id' => $existingTerm->term_id];
                } else {
                    throw new RuntimeException("Failed to get existing term: " . $termData->get_error_message());
                }
            } else {
                throw new RuntimeException("Failed to insert term: " . $termData->get_error_message());
            }
        }

        $termId = $termData['term_id'];

        // Process meta fields
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $metaKey = substr($key, 5); // Remove 'meta.' prefix
                update_term_meta($termId, $metaKey, $value);
            }
        }

        return true;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }
}
