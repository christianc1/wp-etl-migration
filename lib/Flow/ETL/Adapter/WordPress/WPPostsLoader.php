<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;
use Flow\ETL\{FlowContext, Loader, Rows, Row};
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

	public function create_normalizer( FlowContext $context ): RowNormalizer {
		return new RowNormalizer( new EntryNormalizer( $context->config->caster(), $this->dateTimeFormat ) );
	}

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $normalizer = $this->create_normalizer( $context );

        foreach ($rows as $row) {
            $this->insertPost($row, $normalizer);
        }
    }

    public function insertPost(Row | array $row, RowNormalizer | null $normalizer = null ): int
    {
		// Normalize
		if ( $row instanceof Row && $normalizer instanceof RowNormalizer ) {
			$data = $normalizer->normalize( $row );
		} else {
			$data = $row;
		}

        $postData = array_merge($this->postDefaults, array_filter([
            'post_title' => $data['post.post_title'] ?? '',
            'post_content' => $data['post.post_content'] ?? '',
            'post_excerpt' => $data['post.post_excerpt'] ?? '',
            'post_name' => $data['post.post_name'] ?? '',
            'post_status' => $data['post.post_status'] ?? $this->postDefaults['post_status'],
            'post_type' => $data['post.post_type'] ?? $this->postDefaults['post_type'],
            'post_author' => $data['post.post_author'] ?? $this->postDefaults['post_author'],
            'post_date' => $data['post.post_date'] ?? current_time('mysql'),
            'post_date_gmt' => $data['post.post_date_gmt'] ?? get_gmt_from_date($data['post.post_date'] ?? current_time('mysql')),
        ]));

        // Handle post ID if provided (update existing post)
        $postId = $data['post.ID'] ?? $data['post.id'] ?? null;
        if (!empty($postId)) {
            $postData['ID'] = (int) $postId;
        }

        $postId = wp_insert_post($postData, true);

        if (is_wp_error($postId)) {
            throw new RuntimeException("Failed to insert post: " . $postId->get_error_message());
        }

        // Process meta fields
        // foreach ($data as $key => $value) {
        //     if (str_starts_with($key, 'meta.')) {
        //         $metaKey = substr($key, 5); // Remove 'meta.' prefix
        //         update_post_meta($postId, $metaKey, $value);
        //     }
        // }

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
