<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPPostTermsLoader implements Loader
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
            $this->setPostTerms($normalizedRow);
        }
    }

    private function setPostTerms(array $data): bool
    {
        if (empty($data['post.ID'])) {
            throw new RuntimeException('Post ID is required');
        }

        $postId = (int) $data['post.ID'];

        // Verify post exists
        if (!get_post($postId)) {
            throw new RuntimeException("Post with ID {$postId} does not exist");
        }

        $termsUpdated = false;
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'tax.')) {
                $taxonomy = substr($key, 4); // Remove 'tax.' prefix

                // Handle array of terms or single term
                $terms = is_array($value) ? $value : [$value];

                // Filter out empty values
                $terms = array_filter($terms);

                if (empty($terms)) {
                    continue;
                }

                // Verify taxonomy exists
                if (!taxonomy_exists($taxonomy)) {
                    throw new RuntimeException("Taxonomy '{$taxonomy}' does not exist");
                }

                $result = wp_set_object_terms($postId, $terms, $taxonomy);

                if (is_wp_error($result)) {
                    throw new RuntimeException("Failed to set terms for post ID {$postId} with taxonomy {$taxonomy}: " . $result->get_error_message());
                }

                $termsUpdated = true;
            }
        }

        if (!$termsUpdated) {
            throw new RuntimeException('No taxonomy terms found to update');
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
