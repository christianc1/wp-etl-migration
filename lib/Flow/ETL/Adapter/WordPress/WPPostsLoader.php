<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPPostsLoader implements Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;
    private array $postDefaults = [
        'post_status' => 'draft',
        'post_type' => 'post',
        'post_author' => 1,
    ];

    public function __construct(
        private readonly array $config = []
    ) {
        $this->postDefaults = array_merge($this->postDefaults, $config['defaults'] ?? []);
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $normalizer = new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->insertPost($normalizedRow);
        }
    }

    private function insertPost(array $data): int
    {
        $postData = array_merge($this->postDefaults, array_filter([
            'post_title' => $data['post_title'] ?? '',
            'post_content' => $data['post_content'] ?? '',
            'post_excerpt' => $data['post_excerpt'] ?? '',
            'post_name' => $data['post_name'] ?? '',
            'post_status' => $data['post_status'] ?? $this->postDefaults['post_status'],
            'post_type' => $data['post_type'] ?? $this->postDefaults['post_type'],
            'post_author' => $data['post_author'] ?? $this->postDefaults['post_author'],
            'post_date' => $data['post_date'] ?? current_time('mysql'),
            'post_date_gmt' => $data['post_date_gmt'] ?? get_gmt_from_date($data['post_date'] ?? current_time('mysql')),
        ]));

        $postId = wp_insert_post($postData, true);

        if (is_wp_error($postId)) {
            throw new RuntimeException("Failed to insert post: " . $postId->get_error_message());
        }

        // Handle post meta if present
        if (!empty($data['meta']) && is_array($data['meta'])) {
            foreach ($data['meta'] as $meta_key => $meta_value) {
                update_post_meta($postId, $meta_key, $meta_value);
            }
        }

        return $postId;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }

    public function withDefaults(array $defaults): self
    {
        $clone = clone $this;
        $clone->postDefaults = array_merge($this->postDefaults, $defaults);
        return $clone;
    }
}
