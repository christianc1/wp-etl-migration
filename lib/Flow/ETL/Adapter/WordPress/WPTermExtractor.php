<?php
/**
 * WordPress Term Extractor
 *
 * Extracts terms from WordPress taxonomies using Flow ETL.
 *
 * @package Flow\ETL\Adapter\WordPress
 */

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor\{Limitable, LimitableExtractor};
use Flow\ETL\{Extractor, FlowContext, Row, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\Signal;

final class WPTermExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

    private array $termDefaults = [
        'taxonomy' => 'category',
        'hide_empty' => false,
        'orderby' => 'name',
        'order' => 'ASC',
        'number' => 100,
        'offset' => 0,
    ];

    private bool $includeMeta = true;

    public function __construct(
        private readonly array $config = []
    ) {
        $this->termDefaults = array_merge($this->termDefaults, $config['defaults'] ?? []);
        $this->resetLimit();
    }

    public function extract(FlowContext $context): \Generator
    {
        $offset = 0;
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        do {
            $args = array_merge($this->termDefaults, [
                'offset' => $offset,
                'fields' => 'all',
            ]);

            $terms = get_terms($args);

            if (is_wp_error($terms)) {
                throw new RuntimeException('Failed to fetch terms: ' . $terms->get_error_message());
            }

            if (empty($terms)) {
                break;
            }

            foreach ($terms as $term) {
                $termData = $this->normalizeTerm($term);

                if ($this->includeMeta) {
                    $termData['meta'] = get_term_meta($term->term_id);
                }

                if ($shouldPutInputIntoRows) {
                    $termData['_input_source'] = 'wp_terms';
                }

                $signal = yield array_to_rows($termData, $context->entryFactory());
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            $offset += $this->termDefaults['number'];
        } while (count($terms) >= $this->termDefaults['number']);
    }

    private function normalizeTerm(\WP_Term $term): array
    {
        return [
            'term_id' => (int) $term->term_id,
            'name' => $term->name,
            'slug' => $term->slug,
            'term_group' => (int) $term->term_group,
            'term_taxonomy_id' => (int) $term->term_taxonomy_id,
            'taxonomy' => $term->taxonomy,
            'description' => $term->description,
            'parent' => (int) $term->parent,
            'count' => (int) $term->count,
        ];
    }

    public function withTaxonomy(string $taxonomy): self
    {
        $clone = clone $this;
        $clone->termDefaults['taxonomy'] = $taxonomy;
        return $clone;
    }

    public function withHideEmpty(bool $hideEmpty): self
    {
        $clone = clone $this;
        $clone->termDefaults['hide_empty'] = $hideEmpty;
        return $clone;
    }

    public function withOrderBy(string $orderBy): self
    {
        $clone = clone $this;
        $clone->termDefaults['orderby'] = $orderBy;
        return $clone;
    }

    public function withOrder(string $order): self
    {
        $clone = clone $this;
        $clone->termDefaults['order'] = strtoupper($order);
        return $clone;
    }

    public function withNumber(int $number): self
    {
        $clone = clone $this;
        $clone->termDefaults['number'] = $number;
        return $clone;
    }

    public function withMeta(bool $includeMeta = true): self
    {
        $clone = clone $this;
        $clone->includeMeta = $includeMeta;
        return $clone;
    }
}
